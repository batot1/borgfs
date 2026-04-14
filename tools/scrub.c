// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 Bartosz Bartczak
//
// scrub.c — verifies integrity of borgfs store with optional hash verification
// Usage: ./scrub [-q] [-H] /path/to/borgfs_store
// Version 3

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdint.h>
#include <time.h>
#include <blake3.h>

#define BORGFS_META_DIR ".borgfs_meta"
#define BORGFS_CHUNK_DIR ".borgfs_chunks"
#define PATH_MAX 4096

static int errors = 0;
static long total_files = 0;
static long total_chunks = 0;
static long missing_chunks = 0;
static long size_mismatch = 0;
static long hash_mismatch = 0;
static long processed_files = 0;
static long total_meta_files = 0;
static int quiet = 0;
static int verify_hash = 0;  // -H
static time_t start_time = 0;

static void path_chunk(const char *store, const char *hex, char out[PATH_MAX]) {
    snprintf(out, PATH_MAX, "%s/%s/%2.2s/%s", store, BORGFS_CHUNK_DIR, hex, hex);
}

static void blake3_hex(const uint8_t *data, size_t len, char out_hex[65]){
    uint8_t out[32];
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, data, len);
    blake3_hasher_finalize(&hasher, out, sizeof(out));
    static const char hex[] = "0123456789abcdef";
    for (size_t i = 0; i < sizeof(out); i++) {
        out_hex[i*2]   = hex[out[i] >> 4];
        out_hex[i*2+1] = hex[out[i] & 0xF];
    }
    out_hex[64] = '\0';
}

static void update_progress(void) {
    if (quiet || total_meta_files <= 0) return;
    long percent = (processed_files * 100) / total_meta_files;
    static long last_percent = -1;
    if (percent != last_percent && percent % 1 == 0) {
        time_t now = time(NULL);
        double elapsed = difftime(now, start_time);
        double eta = (elapsed / processed_files) * (total_meta_files - processed_files);
        long eta_sec = (long)eta;
        long eta_min = eta_sec / 60;
        long eta_rem_sec = eta_sec % 60;

        fprintf(stderr, "\rScrubbing... [%3ld%%] %ld/%ld files", percent, processed_files, total_meta_files);
        if (eta_sec > 0) {
            if (eta_min > 0)
                fprintf(stderr, " (ETA: %ldm%02lds)", eta_min, eta_rem_sec);
            else
                fprintf(stderr, " (ETA: %lds)", eta_rem_sec);
        }
        fflush(stderr);
        last_percent = percent;
    }
}

static void path_meta(const char *store, const char *relpath, char out[PATH_MAX]) {
    snprintf(out, PATH_MAX, "%s/%s/%s.meta", store, BORGFS_META_DIR, relpath);
}

static void count_meta_files(const char *meta_dir) {
    DIR *d = opendir(meta_dir);
    if (!d) return;
    struct dirent *de;
    while ((de = readdir(d))) {
        if (de->d_name[0] == '.') continue;
        char full[PATH_MAX];
        snprintf(full, PATH_MAX, "%s/%s", meta_dir, de->d_name);
        struct stat st;
        if (stat(full, &st) != 0) continue;
        if (S_ISDIR(st.st_mode)) {
            count_meta_files(full);
        } else if (S_ISREG(st.st_mode)) {
            size_t len = strlen(de->d_name);
            if (len > 5 && strcmp(de->d_name + len - 5, ".meta") == 0) {
                total_meta_files++;
            }
        }
    }
    closedir(d);
}

static void scan_meta_dir(const char *store, const char *meta_dir, const char *prefix) {
    DIR *d = opendir(meta_dir);
    if (!d) {
        if (errno == ENOENT) return;
        if (!quiet) fprintf(stderr, "\nCannot open %s: %s\n", meta_dir, strerror(errno));
        errors++;
        return;
    }

    struct dirent *de;
    while ((de = readdir(d))) {
        if (de->d_name[0] == '.') continue;

        char full_path[PATH_MAX];
        if (prefix[0])
            snprintf(full_path, PATH_MAX, "%s/%s", prefix, de->d_name);
        else
            snprintf(full_path, PATH_MAX, "%s", de->d_name);

        char meta_path[PATH_MAX];
        snprintf(meta_path, PATH_MAX, "%s/%s", meta_dir, de->d_name);

        struct stat st;
        if (stat(meta_path, &st) != 0) continue;

        if (S_ISDIR(st.st_mode)) {
            scan_meta_dir(store, meta_path, full_path);
        } else if (S_ISREG(st.st_mode)) {
            size_t len = strlen(de->d_name);
            if (len > 5 && strcmp(de->d_name + len - 5, ".meta") == 0) {
                processed_files++;
                update_progress();

                char relpath[PATH_MAX];
                strncpy(relpath, full_path, PATH_MAX - 1);
                relpath[PATH_MAX - 1] = '\0';
                relpath[len - 5] = '\0';

                total_files++;
                FILE *f = fopen(meta_path, "rb");
                if (!f) {
                    if (!quiet) fprintf(stderr, "\nCannot open meta: %s\n", meta_path);
                    errors++;
                    continue;
                }

                size_t n;
                if (fscanf(f, "%zu\n", &n) != 1) {
                    if (!quiet) fprintf(stderr, "\nInvalid meta header: %s\n", meta_path);
                    fclose(f);
                    errors++;
                    continue;
                }

                for (size_t i = 0; i < n; i++) {
                    size_t sz;
                    char hex[65];
                    if (fscanf(f, "%zu %64s\n", &sz, hex) != 2) {
                        if (!quiet) fprintf(stderr, "\nInvalid chunk line in %s (line %zu)\n", meta_path, i+2);
                        errors++;
                        break;
                    }

                    total_chunks++;
                    char chunk_path[PATH_MAX];
                    path_chunk(store, hex, chunk_path);

                    struct stat cs;
                    if (stat(chunk_path, &cs) != 0) {
                        if (!quiet) fprintf(stderr, "\nMISSING chunk: %s (referenced by %s)\n", chunk_path, relpath);
                        missing_chunks++;
                        errors++;
                        continue;
                    }

                    if ((size_t)cs.st_size != sz) {
                        if (!quiet) fprintf(stderr, "\nSIZE MISMATCH: %s (expected %zu, got %ld)\n", chunk_path, sz, (long)cs.st_size);
                        size_mismatch++;
                        errors++;
                        continue;
                    }

                    // --- Full hash verification (optional) ---
                    if (verify_hash) {
                        FILE *cf = fopen(chunk_path, "rb");
                        if (!cf) {
                            if (!quiet) fprintf(stderr, "\nCannot open chunk for hash: %s\n", chunk_path);
                            errors++;
                            continue;
                        }
                        uint8_t *buf = malloc(sz);
                        if (!buf || fread(buf, 1, sz, cf) != sz) {
                            if (!quiet) fprintf(stderr, "\nRead error in chunk: %s\n", chunk_path);
                            free(buf);
                            fclose(cf);
                            errors++;
                            continue;
                        }
                        fclose(cf);
                        char actual_hex[65];
                        blake3_hex(buf, sz, actual_hex);
                        if (strcmp(actual_hex, hex) != 0) {
                            if (!quiet) fprintf(stderr, "\nHASH MISMATCH: %s\n", chunk_path);
                            hash_mismatch++;
                            errors++;
                        }
                        free(buf);
                    }
                }
                fclose(f);
            }
        }
    }
    closedir(d);
}

int main(int argc, char *argv[]) {
    int opt;
    while ((opt = getopt(argc, argv, "qH")) != -1) {
        switch (opt) {
            case 'q':
                quiet = 1;
                break;
            case 'H':
                verify_hash = 1;
                break;
            default:
                fprintf(stderr, "Usage: %s [-q] [-H] /path/to/borgfs_store\n", argv[0]);
                return 1;
        }
    }

    if (optind >= argc) {
        fprintf(stderr, "Usage: %s [-q] [-H] /path/to/borgfs_store\n", argv[0]);
        return 1;
    }

    const char *store = argv[optind];
    char meta_root[PATH_MAX];
    snprintf(meta_root, PATH_MAX, "%s/%s", store, BORGFS_META_DIR);

    struct stat st;
    if (stat(meta_root, &st) != 0 || !S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Error: '%s' is not a valid borgfs store (missing %s directory)\n", store, BORGFS_META_DIR);
        return 1;
    }

    start_time = time(NULL);

    if (!quiet) fprintf(stderr, "Counting files...\n");
    count_meta_files(meta_root);
    if (total_meta_files == 0) {
        if (!quiet) fprintf(stderr, "No .meta files found.\n");
        return 0;
    }

    if (!quiet) {
        if (verify_hash)
            fprintf(stderr, "Scanning %ld files with HASH VERIFICATION...\n", total_meta_files);
        else
            fprintf(stderr, "Scanning %ld files...\n", total_meta_files);
    }

    scan_meta_dir(store, meta_root, "");

    if (!quiet) fprintf(stderr, "\n\n=== SCRUB REPORT ===\n");
    printf("Files checked: %ld\n", total_files);
    printf("Chunks checked: %ld\n", total_chunks);
    printf("Missing chunks: %ld\n", missing_chunks);
    printf("Size mismatches: %ld\n", size_mismatch);
    printf("Hash mismatches: %ld\n", hash_mismatch);
    printf("Total errors: %d\n", errors);

    return (errors > 0) ? 1 : 0;
}
