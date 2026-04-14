// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 Bartosz Bartczak
//
// borgfs-gc-fast.c — TRUE scalable AND FAST orphaned chunk detector
// Usage: ./borgfs-gc-fast [-d] /path/to/borgfs_store
//        ./borgfs-gc-fast DEBUG /path/to/borgfs_store   (shows real memory usage and timing)
// Version 4

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <errno.h>
#include <time.h>
#include <search.h>   // hcreate, hsearch
#define BORGFS_META_DIR ".borgfs_meta"
#define BORGFS_CHUNK_DIR ".borgfs_chunks"
#define PATH_MAX 4096

static long total_chunks_on_disk = 0;
static long orphaned_chunks = 0;
static long processed_prefixes = 0;
static long total_prefixes = 0;
static int delete_mode = 0;
static int debug_mode = 0;
static time_t start_time;
static size_t current_ram_usage = 0; // actual RAM usage for the current prefix

// Collect existing prefixes (2-character directories in .borgfs_chunks/)
static void collect_prefixes(const char *store, char prefixes[256][3], size_t *count) {
    char chunks_dir[PATH_MAX];
    snprintf(chunks_dir, PATH_MAX, "%s/%s", store, BORGFS_CHUNK_DIR);
    
    DIR *d = opendir(chunks_dir);
    if (!d) {
        *count = 0;
        return;
    }
    
    struct dirent *de;
    *count = 0;
    while ((de = readdir(d))) {
        if (de->d_name[0] == '.' || strlen(de->d_name) != 2) continue;
        if (*count < 256) {
            strncpy(prefixes[*count], de->d_name, 3);
            (*count)++;
        }
    }
    closedir(d);
}

// Load ONLY chunks for a given prefix
static size_t load_chunks_for_prefix(const char *meta_dir, const char *prefix) {
    size_t count = 0;
    DIR *d = opendir(meta_dir);
    if (!d) return 0;
    
    struct dirent *de;
    while ((de = readdir(d))) {
        if (de->d_name[0] == '.') continue;
        char full[PATH_MAX];
        snprintf(full, PATH_MAX, "%s/%s", meta_dir, de->d_name);
        struct stat st;
        if (stat(full, &st) != 0) continue;
        
        if (S_ISDIR(st.st_mode)) {
            count += load_chunks_for_prefix(full, prefix);
        } else if (S_ISREG(st.st_mode)) {
            size_t len = strlen(de->d_name);
            if (len > 5 && strcmp(de->d_name + len - 5, ".meta") == 0) {
                FILE *f = fopen(full, "rb");
                if (!f) continue;
                size_t n = 0;
                if (fscanf(f, "%zu\n", &n) == 1) {
                    for (size_t i = 0; i < n; i++) {
                        size_t sz;
                        char hex[65];
                        if (fscanf(f, "%zu %64s\n", &sz, hex) == 2) {
                            // Only chunks for this prefix
                            if (strncmp(hex, prefix, 2) == 0) {
                                char *copy = strdup(hex);
                                current_ram_usage += strlen(copy) + 1;
                                ENTRY e = { .key = copy, .data = NULL };
                                hsearch(e, ENTER);
                                count++;
                            }
                        }
                    }
                }
                fclose(f);
            }
        }
    }
    closedir(d);
    return count;
}

// Process a single prefix
static void process_prefix(const char *store, const char *prefix) {
    char meta_root[PATH_MAX];
    snprintf(meta_root, PATH_MAX, "%s/%s", store, BORGFS_META_DIR);
    
    // 1. Clear the state of the previous prefix
    current_ram_usage = 0;
    hdestroy();
    
    // 2. Calculate the hash table size for this prefix
    size_t estimated_chunks = 0;
    DIR *d_meta = opendir(meta_root);
    if (d_meta) {
        // Quick count of chunks for this prefix
        estimated_chunks = load_chunks_for_prefix(meta_root, prefix);
        closedir(d_meta);
    }
    
    size_t table_size = estimated_chunks * 2;
    if (table_size < 1024) table_size = 1024;
    
    // 3. Create the hash table
    if (!hcreate(table_size)) {
        perror("hcreate");
        return;
    }
    
    // 4. Load chunks for this prefix
    load_chunks_for_prefix(meta_root, prefix);
    
    if (debug_mode) {
        fprintf(stderr, "Prefix %s: loaded %zu chunks (%.1f MB RAM)\n", 
                prefix, estimated_chunks, current_ram_usage / (1024.0 * 1024.0));
    }
    
    // 5. Scan the chunks directory for this prefix
    char chunks_dir[PATH_MAX];
    snprintf(chunks_dir, PATH_MAX, "%s/%s/%s", store, BORGFS_CHUNK_DIR, prefix);
    
    DIR *d_chunks = opendir(chunks_dir);
    if (!d_chunks) return;
    
    struct dirent *de;
    while ((de = readdir(d_chunks))) {
        if (de->d_name[0] == '.') continue;
        total_chunks_on_disk++;
        
        ENTRY e = { .key = de->d_name };
        if (hsearch(e, FIND) == NULL) {
            orphaned_chunks++;
            char chunk_path[PATH_MAX];
            snprintf(chunk_path, PATH_MAX, "%s/%s", chunks_dir, de->d_name);
            if (delete_mode) {
                if (unlink(chunk_path) == 0) {
                    printf("DELETED: %s\n", chunk_path);
                } else {
                    fprintf(stderr, "Cannot delete %s: %s\n", chunk_path, strerror(errno));
                }
            } else {
                printf("ORPHAN: %s\n", chunk_path);
            }
        }
    }
    closedir(d_chunks);
    
    // 6. Update progress
    processed_prefixes++;
    double percent = (processed_prefixes * 100.0) / total_prefixes;
    time_t now = time(NULL);
    double elapsed = difftime(now, start_time);
    double eta = 0.0;
    if (elapsed > 0.001 && processed_prefixes > 0) {
        eta = (elapsed / processed_prefixes) * (total_prefixes - processed_prefixes);
    }
    
    fprintf(stderr, "\rProcessing prefixes... [%3.0f%%] %ld/%ld", 
            percent, processed_prefixes, total_prefixes);
    if (eta > 0) {
        long eta_sec = (long)eta;
        if (eta_sec >= 60)
            fprintf(stderr, " (ETA: %ldm%02lds)", eta_sec / 60, eta_sec % 60);
        else
            fprintf(stderr, " (ETA: %lds)", eta_sec);
    }
    fflush(stderr);
    
    // 7. Destroy the hash table (free memory)
    hdestroy();
}

int main(int argc, char *argv[]) {
    // Handle modes
    if (argc >= 2 && strcmp(argv[1], "DEBUG") == 0) {
        debug_mode = 1;
        optind = 2;
    } else {
        int opt;
        while ((opt = getopt(argc, argv, "d")) != -1) {
            if (opt == 'd') delete_mode = 1;
            else {
                fprintf(stderr, "Usage: %s [-d] /path/to/borgfs_store\n", argv[0]);
                return 1;
            }
        }
    }
    
    if (optind >= argc) {
        fprintf(stderr, "Usage: %s [-d] /path/to/borgfs_store\n", argv[0]);
        fprintf(stderr, "       %s DEBUG /path/to/borgfs_store\n", argv[0]);
        return 1;
    }
    
    const char *store = argv[optind];
    char meta_root[PATH_MAX];
    snprintf(meta_root, PATH_MAX, "%s/%s", store, BORGFS_META_DIR);
    
    struct stat st;
    if (stat(meta_root, &st) != 0 || !S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Error: '%s' is not a valid borgfs store\n", store);
        return 1;
    }
    
    start_time = time(NULL);
    fprintf(stderr, "Starting garbage collection (FAST VERSION)...\n");
    
    if (debug_mode) {
        fprintf(stderr, "DEBUG mode enabled\n");
    }
    
    // Collect existing prefixes
    char prefixes[256][3];
    collect_prefixes(store, prefixes, &total_prefixes);
    
    if (total_prefixes == 0) {
        fprintf(stderr, "No prefixes found. Nothing to do.\n");
        return 0;
    }
    
    fprintf(stderr, "Found %ld prefixes to process...\n", total_prefixes);
    
    // Process each prefix sequentially
    for (size_t i = 0; i < total_prefixes; i++) {
        process_prefix(store, prefixes[i]);
    }
    
    fprintf(stderr, "\n\n=== GARBAGE COLLECTOR REPORT ===\n");
    printf("Total chunks on disk: %ld\n", total_chunks_on_disk);
    printf("Orphaned chunks: %ld\n", orphaned_chunks);
    if (delete_mode) {
        printf("Orphaned chunks DELETED.\n");
    } else {
        printf("Dry-run mode (use -d to delete).\n");
    }
    
    return 0;
}
