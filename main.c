// SPDX-License-Identifier: AGPL-3.0-or-later 
// Copyright (c) 2025 Bartosz Bartczak 
// 
// BorgFS is a lightweight, deduplicating file system built from the ground up 
// for massive data deduplication. It serves as a highly efficient alternative 
// to existing solutions by leveraging the FastCDC algorithm and BLAKE3 hashing.
// 
// main.c (formerly borgfs.c 0.2.003)
// 
// TODO: Address very slow file read performance on HDDs. 
// After adding caching, the issue remains only on cold starts.
// 
// Minimal prototype: FUSE (libfuse3, high-level API) + FastCDC (Gear) + BLAKE3 
// Supported operations: getattr, readdir, open, read, write, flush/fsync, release, mkdir/unlink/rmdir/statfs 
// Backend: content-addressed store (chunks by BLAKE3 hex) + CDC (FastCDC Gear w/ normalization) 
//
//
// Compilation: 
//   Please use the provided Makefile (simply run `make`).
// 
// Usage: 
//   ./borgfs -o storage=/path/to/store -f /path/to/mountpoint 
// 
// Note: This is a Proof of Concept (PoC); it features simplified attributes, 
// no journaling, no symlink support, etc.

#ifndef NDEBUG
#define DBG(...) do { fprintf(stderr, __VA_ARGS__); fputc('\n', stderr); } while(0)
#else
#define DBG(...) do {} while(0)
#endif

#define FUSE_USE_VERSION 35

#include <fuse3/fuse.h>
//#include <fuse3/fuse_lowlevel.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/statvfs.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <dirent.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <time.h>


#include <blake3.h>   // BLAKE3 (custom library included manually)

// -------------------------- Configuration --------------------------
#define BORGFS_MAX_NAME 255
#define BORGFS_META_DIR ".borgfs_meta"     // file->chunk list maps
#define BORGFS_CHUNK_DIR ".borgfs_chunks"  // chunk files by hash
#define FD_CACHE_SIZE 512                  // number of open files

// CDC parameters
#define CHUNK_MIN   2048u
#define CHUNK_AVG   8192u   // must be a power of 2
#define CHUNK_MAX   65536u



// =========================================================
// Time measurement (timers) — activate via -DBORGFS_TIMER
// =========================================================
#ifdef BORGFS_TIMER
    #define TSTAMP() ({ \
        struct timespec ts; \
        clock_gettime(CLOCK_MONOTONIC, &ts); \
        (double)ts.tv_sec + ts.tv_nsec / 1e9; \
    })
    #define TIMER_LOG(fmt, ...) \
        fprintf(stderr, "[TIMER] " fmt "\n", ##__VA_ARGS__)
#else
    #define TSTAMP() (0.0)
    #define TIMER_LOG(fmt, ...) ((void)0)
#endif


// ----------------------- Simple structures -------------------------
typedef struct chunk_ref {
    char   id_hex[65]; // 64 + NUL
    size_t size;
} chunk_ref_t;

typedef struct chunk_list {
    chunk_ref_t *chunks;
    size_t count;
} chunk_list_t;

typedef struct file_ctx {
    uint8_t *data;        // write data buffer (RAM)
    size_t   len;         // number of valid bytes in data
    size_t   cap;         // currently allocated capacity of data
    int      dirty;       // 1 if there is an uncommitted write
    chunk_list_t cl;      // list of file chunks
    size_t *cum;          // length prefixes: cum[i] = sum of size[0..i-1]
    size_t n;             // number of chunks
    size_t cur_idx;       // current chunk for caching
    int    cur_fd;        // open file descriptor of the current chunk
    uint8_t *cur_buf;     // chunk buffer
    size_t cur_len;       // data length in the buffer
} file_ctx_t;

typedef struct {
    char path[PATH_MAX];
    int fd;
    uint64_t last_used;
} fd_entry_t;


// --------------------- Global configuration ----------------------
static char g_storage[PATH_MAX] = {0};
static fd_entry_t fd_cache[FD_CACHE_SIZE];
static uint64_t fd_cache_clock = 0;

// ------------------------- Path helpers ------------------------
/* old version
static void path_chunk(const char *hex, char out[PATH_MAX]) {
    snprintf(out, PATH_MAX, "%s/%s/%2.2s/%s", g_storage, BORGFS_CHUNK_DIR, hex, hex);
}
static void path_meta(const char *rel, char out[PATH_MAX]) {
    snprintf(out, PATH_MAX, "%s/%s/%s.meta", g_storage, BORGFS_META_DIR, rel[0]=='/'? rel+1 : rel);
}
*/
static void path_chunk(char *out, const char *hex) {
    int len = snprintf(out, PATH_MAX, "%s/%s/%2.2s/%s", g_storage, BORGFS_CHUNK_DIR, hex, hex);
    if (len >= PATH_MAX) {
        fprintf(stderr, "Error: Chunk path is too long and was truncated!\n");
    }
}

static void path_meta(char *out, const char *rel) {
    int len = snprintf(out, PATH_MAX, "%s/%s/%s.meta", g_storage, BORGFS_META_DIR, rel[0]=='/'? rel+1 : rel);
    if (len >= PATH_MAX) {
        fprintf(stderr, "Error: Metadata path is too long and was truncated!\n");
    }
}

static void path_meta_dir(const char *rel, char out[PATH_MAX]) {
    if (rel && rel[0] != '\0') {
        snprintf(out, PATH_MAX, "%s/%s/%s", g_storage, BORGFS_META_DIR, rel);
    } else {
        snprintf(out, PATH_MAX, "%s/%s", g_storage, BORGFS_META_DIR);
    }
}


static const char *relpath(const char *path){
    return (*path=='/' && path[1])? path+1 : (*path=='/'? "" : path);
}

static int get_fd_cached(const char *path) {
    fd_cache_clock++;

    // try to find an open one
    for (int i = 0; i < FD_CACHE_SIZE; i++) {
        if (fd_cache[i].fd > 0 && strcmp(fd_cache[i].path, path) == 0) {
            fd_cache[i].last_used = fd_cache_clock;
            return fd_cache[i].fd;
        }
    }

    // find empty or oldest
    int victim = 0;
    uint64_t oldest = UINT64_MAX;
    for (int i = 0; i < FD_CACHE_SIZE; i++) {
        if (fd_cache[i].fd <= 0) { victim = i; break; }
        if (fd_cache[i].last_used < oldest) {
            oldest = fd_cache[i].last_used;
            victim = i;
        }
    }

    // close old FD if need
    if (fd_cache[victim].fd > 0) close(fd_cache[victim].fd);

    // open new
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -errno;
    strncpy(fd_cache[victim].path, path, sizeof(fd_cache[victim].path));
    fd_cache[victim].fd = fd;
    fd_cache[victim].last_used = fd_cache_clock;
    return fd;
}

static void close_all_cached_fds(void) {
    for (int i = 0; i < FD_CACHE_SIZE; i++) {
        if (fd_cache[i].fd > 0) {
            close(fd_cache[i].fd);
            fd_cache[i].fd = 0;
        }
	fd_cache[i].path[0] = '\0';
	fd_cache[i].last_used = 0;
    }
}

// ------------------------ for catalogs ----------------------
static int ensure_dirs(void){
    char p1[PATH_MAX], p2[PATH_MAX];
    snprintf(p1, PATH_MAX, "%s/%s", g_storage, BORGFS_META_DIR);
    snprintf(p2, PATH_MAX, "%s/%s", g_storage, BORGFS_CHUNK_DIR);
    if (mkdir(p1, 0700) && errno != EEXIST) return -errno;
    if (mkdir(p2, 0700) && errno != EEXIST) return -errno;
    return 0;
}

// -------------    mkdir -p for catalogs      ----------------
static int ensure_parent_dirs_of(const char *filepath){
    char tmp[PATH_MAX];
    strncpy(tmp, filepath, PATH_MAX-1);
    tmp[PATH_MAX-1]='\0';
    char *slash = strrchr(tmp, '/');
    if (!slash) return 0;
    *slash = '\0';
    for (char *p = tmp + 1; *p; ++p){
        if (*p == '/'){
            *p = '\0';
            if (mkdir(tmp, 0700) && errno != EEXIST) return -errno;
            *p = '/';
        }
    }
    if (mkdir(tmp, 0700) && errno != EEXIST) return -errno;
    return 0;
}

// --------------------------- BLAKE3 -> hex ------------------------
static void blake3_hex(const uint8_t *data, size_t len, char out_hex[65]){
    uint8_t out[32]; // 256-bit
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, data, len);
    blake3_hasher_finalize(&hasher, out, sizeof(out));
    static const char *hex = "0123456789abcdef";
    for (size_t i=0;i<sizeof(out);i++){
        out_hex[i*2]   = hex[out[i]>>4];
        out_hex[i*2+1] = hex[out[i]&0xF];
    }
    out_hex[64]='\0';
}

// ---------------------------- FastCDC -----------------------------
// Minimal Gear-hash with normalize (mask_strict up to ~2*AVG, then mask_loose).
typedef struct {
   uint64_t gear[256];
   size_t   min_size, avg_size, max_size;
   uint64_t mask_strict, mask_loose;
} fastcdc_ctx;

static inline uint64_t splitmix64(uint64_t *x){
   uint64_t z = (*x += 0x9E3779B97F4A7C15ull);
   z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
   z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
   return z ^ (z >> 31);
}

static inline size_t find_chunk_idx(const size_t *cum, size_t n, size_t off){
   // find:  cum[i] <= off < cum[i+1]
   size_t lo = 0, hi = n;
   while (lo+1 < hi) {
      size_t mid = (lo+hi)/2;
      if (cum[mid] <= off)
	 lo = mid;
      else hi = mid;
   }
   return lo;
}


static unsigned ilog2u(size_t n){
   unsigned b=0;
   while(n>>=1)
      ++b;
   return b;
}

static void fastcdc_init(fastcdc_ctx *c, size_t min_size, size_t avg_size, size_t max_size, uint64_t seed){
   if (min_size < 64) min_size = 64;
   if (avg_size < min_size*2) avg_size = min_size*2;
   if (max_size < avg_size*2) max_size = avg_size*2;
   c->min_size=min_size; c->avg_size=avg_size; c->max_size=max_size;
   unsigned bits = ilog2u(avg_size); if (bits < 6) bits = 6; if (bits > 30) bits = 30;
   c->mask_loose  = (1ull<<bits)   - 1ull;      // ~1/AVG
   c->mask_strict = (1ull<<(bits+1))- 1ull;     // ~1/(2*AVG)
   for (int i=0;i<256;i++){ c->gear[i] = splitmix64(&seed); }
}

static inline uint64_t gear_roll(uint64_t h, uint8_t b, const fastcdc_ctx *c){
   return (h<<1) + c->gear[b];
}

static size_t fastcdc_next(const uint8_t *data, size_t len, const fastcdc_ctx *c){
   if (len <= c->min_size) return len;
   uint64_t h = 0;
   size_t i = 0;
   size_t soft_max = (c->avg_size<<1);
   size_t hard_max = c->max_size;

   // for min_size: don't cut
   for (; i < c->min_size && i < len; ++i) h = gear_roll(h, data[i], c);
   if (i >= len) return len;

   // normalize for ~2*AVG mask strict
   for (; i < len && i < soft_max; ++i){
      h = gear_roll(h, data[i], c);
      if ((h & c->mask_strict) == 0) return i+1;
    }
   // past ~2*AVG for MAX mask loose
   for (; i < len && i < hard_max; ++i){
      h = gear_roll(h, data[i], c);
      if ((h & c->mask_loose) == 0) return i+1;
   }
   // hard cut
   return (len < hard_max ? len : hard_max);
}

static size_t fastcdc_split_points(const uint8_t *buf, size_t len, size_t *out_points, size_t max_points){
    if (len == 0) return 0;
    fastcdc_ctx c; fastcdc_init(&c, CHUNK_MIN, CHUNK_AVG, CHUNK_MAX, 0x12345678u);
    size_t n=0, off=0;
    while (off < len){
        size_t left = len - off;
        size_t clen = fastcdc_next(buf+off, left, &c);
        if (n < max_points) out_points[n] = off + clen;
        ++n; off += clen;
    }
    return n;
}

// CDC function used by commit
static size_t cdc_split_points(const uint8_t *buf, size_t len, size_t *out_points, size_t max_points){
    return fastcdc_split_points(buf, len, out_points, max_points);
}

// ------------------------ I/O chunks -----------------------------
/*		old version with problematic I/O HDD Read=~250kB/s
static int chunk_store(const uint8_t *data, size_t len, char out_id[65]){
    blake3_hex(data, len, out_id);
    char path[PATH_MAX]; path_chunk(out_id, path);
    // prefix directory
    char dirp[PATH_MAX]; snprintf(dirp, PATH_MAX, "%s/%s/%2.2s", g_storage, BORGFS_CHUNK_DIR, out_id);
    if (mkdir(dirp,0700) && errno!=EEXIST) return -errno;
    if (access(path, F_OK)==0) return 0;
    FILE *f=fopen(path, "wb"); if(!f) return -errno;
    size_t w=fwrite(data,1,len,f); if (fflush(f)!=0){ int e=-errno; fclose(f); return e; }
    int fd = fileno(f); if (fd>=0) fsync(fd);
    fclose(f);
    if (w!=len) return -EIO;
    return 0;
}
*/

static int chunk_store(const uint8_t *data, size_t len, char out_id[65]){
    // calculate ID and determine paths
    blake3_hex(data, len, out_id);

    char path[PATH_MAX];
    path_chunk(out_id, path);

    // prefix directory xx/
    char dirp[PATH_MAX];
    snprintf(dirp, PATH_MAX, "%s/%s/%2.2s", g_storage, BORGFS_CHUNK_DIR, out_id);
    if (mkdir(dirp, 0700) && errno != EEXIST)
        return -errno;

    // if chunk already exists – do nothing
    if (access(path, F_OK) == 0)
        return 0;

    // write without fsync/fflush (per-chunk)
    FILE *f = fopen(path, "wb");
    if (!f) return -errno;

    size_t w = fwrite(data, 1, len, f);
    if (w != len) { int e = -errno; fclose(f); return e; }

    if (fclose(f) != 0) return -errno;

    return 0;
}


static ssize_t chunk_load(const char *id_hex, uint8_t *buf, size_t maxlen){
    char path[PATH_MAX]; path_chunk(id_hex, path);
    FILE *f=fopen(path, "rb"); if(!f) return -errno;
    size_t r=fread(buf,1,maxlen,f); fclose(f); return (ssize_t)r;
}

// --------------------- Meta: list chunks per file ----------------
static void free_chunk_list(chunk_list_t *cl){ if (cl->chunks){ free(cl->chunks); cl->chunks=NULL; } cl->count=0; }

static int write_chunk_list(const char *relpath, const chunk_list_t *cl){
    char mp[PATH_MAX]; path_meta(relpath, mp);
    int er = ensure_parent_dirs_of(mp); if (er) return er;

    char tmp[PATH_MAX];
    snprintf(tmp, PATH_MAX, "%s.tmp.%ld", mp, (long)getpid());

    FILE *f = fopen(tmp, "wb");
    if (!f) return -errno;

    if (fprintf(f, "%zu\n", cl->count) < 0) { int e=-errno; fclose(f); unlink(tmp); return e; }
    for (size_t i = 0; i < cl->count; ++i){
        if (fprintf(f, "%zu %s\n", cl->chunks[i].size, cl->chunks[i].id_hex) < 0){
            int e=-errno; fclose(f); unlink(tmp); return e;
        }
    }
    if (fflush(f)!=0){ int e=-errno; fclose(f); unlink(tmp); return e; }
    int fd = fileno(f); if (fd>=0) fsync(fd);
    if (fclose(f)!=0){ int e=-errno; unlink(tmp); return e; }
    if (rename(tmp, mp)!=0){ int e=-errno; unlink(tmp); return e; }

    DBG("[meta] %s: zapisano %zu chunków -> %s", relpath, cl->count, mp);
    return 0;
}

static int read_chunk_list(const char *relpath, chunk_list_t *out){
    memset(out,0,sizeof(*out));
    char mp[PATH_MAX];
    path_meta(relpath, mp);
    FILE *f=fopen(mp, "rb");
    if(!f) return -ENOENT;
    size_t n=0;
    if (fscanf(f, "%zu\n", &n)!=1){
	fclose(f);
	return -EIO;
    }
    out->chunks = (chunk_ref_t*)calloc(n, sizeof(chunk_ref_t));
    if(!out->chunks){
	fclose(f);
	return -ENOMEM;
    }
    out->count = n;
    for (size_t i=0;i<n;i++){
        size_t sz=0;
	char hex[65]={0};
        if (fscanf(f, "%zu %64s\n", &sz, hex)!=2){
	    fclose(f);
	    free(out->chunks);
	    return -EIO;
	}
        out->chunks[i].size=sz;
	strncpy(out->chunks[i].id_hex, hex, 65);
    }
    #ifdef BORGFS_TIMER
       static __thread size_t chunk_reads = 0;
       chunk_reads++;
       if (chunk_reads % 1000 == 0)
          fprintf(stderr, "[READ_CHUNK] total calls: %zu\n", chunk_reads);
       static __thread int open_count = 0;
       open_count++;
       if (open_count % 1000 == 0)
          fprintf(stderr, "[TIMER] read_chunk called %d times so far\n", open_count);
    #endif
    fclose(f); return 0;
}

// ------------------------- FUSE: getattr/readdir ------------------
/*
// Old version working correctly but with code38 and without chmod/chown/utime
static int borgfs_getattr(const char *path, struct stat *st, struct fuse_file_info *fi){
    (void)fi; memset(st,0,sizeof(*st));
    if (strcmp(path, "/")==0){
        st->st_mode = S_IFDIR | 0755;
	st->st_nlink=2;
	return 0;
    }
    chunk_list_t cl;
    if (read_chunk_list(relpath(path), &cl)==0){
        off_t total=0;
	for (size_t i=0;i<cl.count;i++)
           total += cl.chunks[i].size;
        st->st_mode = S_IFREG | 0644;
	st->st_nlink=1;
	st->st_size=total;
        st->st_blksize=4096;
	st->st_blocks=(total+511)/512;
	free_chunk_list(&cl);
	return 0;
    }
    return -ENOENT;
}
*/

// New version that was supposed to solve chmod/chown/utime and it did, but "cd" to "./borgfs_mnt/" stopped working
// If it doesn't solve the problem, restore the previous version
static int borgfs_getattr(const char *path, struct stat *st, struct fuse_file_info *fi){
    (void)fi;
    memset(st, 0, sizeof(*st));

    // root directory
    if (strcmp(path, "/") == 0) {
        st->st_mode  = S_IFDIR | 0755;
        st->st_nlink = 2;
        return 0;
    }

    // meta-path
    char mp[PATH_MAX];
    path_meta(relpath(path), mp);

    // if meta does not exist -> no file
    if (access(mp, F_OK) != 0)
        return -ENOENT;

    // 1) Size from the chunk list (as before)
    off_t total = 0;
    {
        chunk_list_t cl;
        if (read_chunk_list(relpath(path), &cl) != 0)
            return -ENOENT;  // missing meta/broken meta

        for (size_t i = 0; i < cl.count; i++)
            total += (off_t)cl.chunks[i].size;

        free_chunk_list(&cl);
    }

    // 2) Default regular file attributes + size
    st->st_mode    = S_IFREG | 0644;  // type + minimal bits (we will overwrite below from meta)
    st->st_nlink   = 1;
    st->st_size    = total;
    st->st_blksize = 4096;
    st->st_blocks  = (total + 511) / 512;

    // 3) Fetching real attributes from meta (mode/uid/gid/times)
    struct stat mst;
    if (stat(mp, &mst) == 0) {
        // preserve type (S_IFREG), apply bits from meta
        st->st_mode  = (st->st_mode & S_IFMT) | (mst.st_mode & 07777);
        st->st_uid   = mst.st_uid;
        st->st_gid   = mst.st_gid;
        st->st_atime = mst.st_atime;
        st->st_mtime = mst.st_mtime;
        st->st_ctime = mst.st_ctime;
    } else {
        // fallback: at least the uid/gid of the user calling FUSE
        struct fuse_context *fc = fuse_get_context();
        if (fc) {
            st->st_uid = fc->uid;
            st->st_gid = fc->gid;
        }
        // times will remain 0 — OK for start
    }

    return 0;
}



static int borgfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t off, struct fuse_file_info *fi, enum fuse_readdir_flags flags)
{
    (void)off; (void)fi; (void)flags;
    if (strcmp(path, "/") != 0) return -ENOENT;

    filler(buf, ".",  NULL, 0, 0);
    filler(buf, "..", NULL, 0, 0);

    char metaroot[PATH_MAX];
    snprintf(metaroot, PATH_MAX, "%s/%s", g_storage, BORGFS_META_DIR);

    DIR *d = opendir(metaroot);
    if (!d) return 0;

    struct dirent *de;
    const char *suf = ".meta";
    size_t slen = strlen(suf);

    while ((de = readdir(d))) {
        const char *dn = de->d_name;
        if (dn[0] == '.') continue;

        size_t nlen = strlen(dn);
        if (nlen > slen && strcmp(dn + (nlen - slen), suf) == 0) {
            // Strip .meta
            size_t baselen = nlen - slen;
            if (baselen > BORGFS_MAX_NAME) baselen = BORGFS_MAX_NAME;

            char name[BORGFS_MAX_NAME+1];
            memcpy(name, dn, baselen);
            name[baselen] = '\0';

            // Minimal stat (getattr will provide details)
            struct stat st; memset(&st, 0, sizeof(st));
            st.st_mode  = S_IFREG | 0644;
            st.st_nlink = 1;
            filler(buf, name, &st, 0, 0);
        }
        // (Ignoring other files in .borgfs_meta)
    }

    closedir(d);
    return 0;
}


// --------------------------- open/create --------------------------
static int write_empty_meta(const char *relpath){
    char mp[PATH_MAX];
    path_meta(relpath, mp);
    int er = ensure_parent_dirs_of(mp);
    if (er) return er;
    FILE *f = fopen(mp, "wb");
    if (!f) return -errno;
    fprintf(f, "0\n");
    fclose(f);
    return 0;
}

static int borgfs_open(const char *path, struct fuse_file_info *fi){
    char mp[PATH_MAX];
    path_meta(relpath(path), mp);

    // If meta doesn't exist and O_CREAT is missing —> ENOENT
    if (access(mp, F_OK) != 0 && !(fi->flags & O_CREAT))
        return -ENOENT;

    file_ctx_t *ctx = (file_ctx_t*)calloc(1, sizeof(file_ctx_t));
    if (!ctx)
        return -ENOMEM;

    // Load meta for reading (if file exists) and count prefixes
    chunk_list_t cl = (chunk_list_t){0};
    if (read_chunk_list(relpath(path), &cl) == 0) {
        ctx->cl = cl;
        ctx->n  = cl.count;
        if (ctx->n) {
            ctx->cum = (size_t*)calloc(ctx->n + 1, sizeof(size_t));
            if (!ctx->cum) {
                free_chunk_list(&ctx->cl);
                free(ctx);
                return -ENOMEM;
            }
            for (size_t i = 0; i < ctx->n; ++i)
                ctx->cum[i + 1] = ctx->cum[i] + ctx->cl.chunks[i].size;
        }
    }
    // Otherwise: new file — ctx->n==0, cum==NULL, OK.

    // Chunk cache – initialization ONE time
    ctx->cur_idx = (size_t)-1;
    ctx->cur_fd  = -1;
    ctx->cur_buf = NULL;
    ctx->cur_len = 0;

    // (write buffer is zeroed by calloc)
    // ctx->data=NULL; ctx->len=0; ctx->cap=0; ctx->dirty=0;
    
    fi->fh = (uint64_t)(uintptr_t)ctx;
    return 0;
}


static int borgfs_create(const char *path, mode_t mode, struct fuse_file_info *fi){
    (void)mode;
    int rc = write_empty_meta(relpath(path));
    if (rc)
      return rc;
    return borgfs_open(path, fi);
}

static int borgfs_truncate(const char *path, off_t size, struct fuse_file_info *fi){
    (void)fi;
    if (size == 0)
      return write_empty_meta(relpath(path));
    return 0; // PoC: no real shortening; RAM buffer will be overwritten
}

// ------------------------------ write -----------------------------
static int borgfs_write(const char *path, const char *buf, size_t size, off_t off, struct fuse_file_info *fi){
    (void)path;
    file_ctx_t *ctx = (file_ctx_t*)(uintptr_t)fi->fh;
    if (!ctx)
      return -EIO;

    // [1] Reject overflow immediately
    if (off < 0)
	return -EINVAL;
    size_t offu = (size_t)off;
    if (offu > SIZE_MAX - size)
	return -EFBIG;

    size_t need = offu + size;

    // [2] Realloc with overflow protection
    if (ctx->cap < need){
        size_t newcap = ctx->cap ? ctx->cap : CHUNK_MAX;
        while (newcap < need){
            if (newcap > SIZE_MAX/2) return -EFBIG;
            newcap *= 2;
        }
        uint8_t *nd = (uint8_t*)realloc(ctx->data, newcap);
        if (!nd) return -ENOMEM;
        if (newcap > ctx->cap) memset(nd + ctx->cap, 0, newcap - ctx->cap);
        ctx->data = nd; ctx->cap = newcap;
    }

    // [3] Hole
    if (offu > ctx->len) memset(ctx->data + ctx->len, 0, offu - ctx->len);

    // [4] Write
    memcpy(ctx->data + offu, buf, size);
    size_t newlen = offu + size; if (newlen > ctx->len) ctx->len = newlen;

    DBG("[write] %s += %zu @%llu (total=%zu)", path, size, (unsigned long long)off, ctx->len);
    return (int)size;
}

// ------------------------------ commit ----------------------------
static int borgfs_commit_ctx(const char *path, struct fuse_file_info *fi){
    file_ctx_t *ctx = (file_ctx_t*)(uintptr_t)fi->fh;
    if (!ctx || ctx->len == 0) return 0;

    size_t maxp = ctx->len / CHUNK_MIN + 2;
    size_t *points = (size_t*)malloc(sizeof(size_t)*maxp);
    if (!points) return -ENOMEM;

    size_t n = cdc_split_points(ctx->data, ctx->len, points, maxp);
    DBG("[commit] %s: len=%zu -> chunks=%zu", path, ctx->len, n);

    chunk_list_t cl = { .chunks = (chunk_ref_t*)calloc(n, sizeof(chunk_ref_t)), .count = n };
    if (!cl.chunks){ free(points); return -ENOMEM; }

    size_t start=0;
    for (size_t i=0;i<n;i++){
        size_t end = points[i], sz = end - start;
        char hex[65];
        int rc = chunk_store(ctx->data+start, sz, hex);
        if (rc){ free(points); free_chunk_list(&cl); return rc; }
        strncpy(cl.chunks[i].id_hex, hex, 65);
        cl.chunks[i].size = sz;
        start = end;
    }
    int r = write_chunk_list(relpath(path), &cl);
    free(points); free_chunk_list(&cl);

    // after commit, clear the buffer — PoC
    free(ctx->data); ctx->data=NULL; ctx->len=ctx->cap=0;
    return r;
}

static int borgfs_flush(const char *path, struct fuse_file_info *fi){
    (void)path;
    return borgfs_commit_ctx(path, fi);
}

static int borgfs_release(const char *path, struct fuse_file_info *fi){
    (void)path;

    // just in case: commit (if anything is left in the buffer)
    int rc = borgfs_commit_ctx(path, fi);

    file_ctx_t *ctx = (file_ctx_t*)(uintptr_t)fi->fh;
    if (ctx) {
        if (ctx->cur_buf) free(ctx->cur_buf);
        if (ctx->cum) free(ctx->cum);
        // you free ctx->cl where usual (free_chunk_list), if you are already doing so
        free(ctx);
        fi->fh = 0;
    }
    return rc;
}

static void borgfs_destroy(void *private_data){
    (void)private_data;
    close_all_cached_fds();
}

static int borgfs_fsync(const char *path, int isdatasync, struct fuse_file_info *fi){
    (void)isdatasync;
    return borgfs_flush(path, fi);
}

// ---------------------------- flock ------------------------------

static int borgfs_flock(const char *path, struct fuse_file_info *fi, int op)                                                                                                                  
{                                 
    (void)path;                 
    (void)fi;                   
    (void)op;                                 
    return 0; // NoC
} 


// ---------------------------- read/statfs -------------------------

static int borgfs_read(const char *path, char *buf, size_t size, off_t off, struct fuse_file_info *fi)
{
    (void)path;

#ifdef BORGFS_TIMER
    double t0 = TSTAMP();
#endif

    file_ctx_t *ctx = (file_ctx_t*)(uintptr_t)fi->fh;
    if (!ctx || ctx->n == 0) {
#ifdef BORGFS_TIMER
       double t1 = TSTAMP();
       TIMER_LOG("read %s: %.6f s (offset=%zu, size=%zu) [EMPTY]", path, t1-t0, (size_t)off, (size_t)size);
#endif
       return 0;
    }

    size_t fsize = ctx->cum[ctx->n];
    if ((size_t)off >= fsize) {
#ifdef BORGFS_TIMER
        double t1 = TSTAMP();
        TIMER_LOG("read %s: %.6f s (offset=%zu, size=%zu) [EOF]", path, t1-t0, (size_t)off, (size_t)size);
#endif
        return 0;
    }
    if ((size_t)off + size > fsize)
	size = fsize - (size_t)off;

    // find the starting chunk
#ifdef BORGFS_TIMER
    double t_find = TSTAMP();
#endif
    size_t i = find_chunk_idx(ctx->cum, ctx->n, (size_t)off);
#ifdef BORGFS_TIMER
    double t_find2 = TSTAMP();
    TIMER_LOG("find_chunk_idx: %.6f s", t_find2 - t_find);
#endif

    size_t pos     = ctx->cum[i];     // beginning of chunk i
    size_t req     = (size_t)off;     // current request offset
    size_t remain  = size;            // how much left to copy
    size_t out_off = 0;               // offset in the output buffer

    while (remain > 0 && i < ctx->n) {
        const size_t chunk_size = ctx->cl.chunks[i].size;
        const size_t chunk_off  = ctx->cum[i];

        // offset inside the chunk
        size_t start_in_chunk = (req > chunk_off) ? (req - chunk_off) : 0;
        if (start_in_chunk > chunk_size) start_in_chunk = chunk_size;

        size_t avail = chunk_size - start_in_chunk;
        size_t tocpy = (avail < remain) ? avail : remain;

        // if chunk changed -> prefetch the entire chunk into ctx->cur_buf
        if (ctx->cur_idx != i) {
            // prepare buffer for the chunk
            if (!ctx->cur_buf || ctx->cur_len < chunk_size) {
                uint8_t *nb = (uint8_t*)realloc(ctx->cur_buf, chunk_size);
                if (!nb) return -ENOMEM;
                ctx->cur_buf = nb;
                ctx->cur_len = chunk_size;
            }

            // open (from cache) and read chunk
            char cp[PATH_MAX];
            path_chunk(ctx->cl.chunks[i].id_hex, cp);

#ifdef BORGFS_TIMER
            double t_open0 = TSTAMP();
#endif
            int fd = get_fd_cached(cp);
            if (fd < 0) return fd;
#ifdef BORGFS_TIMER
            double t_open1 = TSTAMP();
            TIMER_LOG("chunk open/cache hit: %.6f s [%s]", t_open1 - t_open0, cp);
#endif

#ifdef BORGFS_TIMER
            double t_pread0 = TSTAMP();
#endif
            ssize_t rd = pread(fd, ctx->cur_buf, chunk_size, 0);
            if (rd < 0) return -errno;
            if ((size_t)rd != chunk_size) return -EIO;
#ifdef BORGFS_TIMER
            double t_pread1 = TSTAMP();
            TIMER_LOG("chunk pread(%zu): %.6f s [%s]", chunk_size, t_pread1 - t_pread0, cp);
#endif

            ctx->cur_idx = i;
            // ctx->cur_fd is not needed here: FD is kept in the global cache, so we close nothing
        }

        // copy from the chunk buffer to the output buffer
        memcpy(buf + out_off, ctx->cur_buf + start_in_chunk, tocpy);

        out_off += tocpy;
        req     += tocpy;
        remain  -= tocpy;

        if (remain == 0) break;

        // next chunk
        i++;
        pos = ctx->cum[i];
    }

#ifdef BORGFS_TIMER
    double t1 = TSTAMP();
    TIMER_LOG("read %s: %.6f s (offset=%zu, size=%zu) -> copied=%zu", path, t1 - t0, (size_t)off, (size_t)size, out_off);
#endif
    return (int)out_off;
}


static int borgfs_statfs(const char *path, struct statvfs *st){
    (void)path;
    // try to fetch statvfs from g_storage – then df sees the size
    struct statvfs under;
    if (statvfs(g_storage, &under) == 0){
        *st = under;
        st->f_namemax = BORGFS_MAX_NAME;
        return 0;
    }
    memset(st,0,sizeof(*st));
    st->f_bsize   = 4096;
    st->f_frsize  = 4096;
    st->f_blocks  = 1024;   // pro forma
    st->f_bfree   = 1024;
    st->f_bavail  = 1024;
    st->f_files   = 1000000;
    st->f_ffree   = 900000;
    st->f_namemax = BORGFS_MAX_NAME;
    return 0;
}

// ------------------------ unlink/mkdir/rmdir ----------------------
static int borgfs_unlink(const char *path){
    // delete meta; we don't garbage-collect chunks (PoC)
    char mp[PATH_MAX]; path_meta(relpath(path), mp);
    if (unlink(mp) != 0) return -errno;
    return 0;
}

static int prune_empty_parents(const char *full, const char *stop){
    // go up and remove empty directories until 'stop' (do not remove stop)
    char path[PATH_MAX]; strncpy(path, full, PATH_MAX-1); path[PATH_MAX-1]='\0';
    while (1){
        char *slash = strrchr(path, '/'); if (!slash) break;
        if (strcmp(path, stop) == 0) break;
        *slash = '\0';
        DIR *d = opendir(path);
        if (!d) break;
        struct dirent *de; int empty = 1;
        while ((de = readdir(d))){
            if (strcmp(de->d_name,".")==0 || strcmp(de->d_name,"..")==0) continue;
            empty = 0; break;
        }
        closedir(d);
        if (!empty) break;
        if (rmdir(path) != 0) break;
    }
    return 0;
}

static int borgfs_mkdir(const char *path, mode_t mode){
    (void)mode;
    char md[PATH_MAX]; path_meta_dir(relpath(path), md);
    if (mkdir(md, 0700) != 0) return -errno;
    return 0;
}

static int borgfs_rmdir(const char *path){
    char md[PATH_MAX]; path_meta_dir(relpath(path), md);
    // Remove only if empty (meaning no .meta in subtree)
    char root[PATH_MAX]; snprintf(root, PATH_MAX, "%s/%s", g_storage, BORGFS_META_DIR);
    size_t L = strlen(root);

    // check recursively if anything is underneath
    DIR *d = opendir(md);
    if (d){
        struct dirent *de;
        while ((de = readdir(d))){
            if (strcmp(de->d_name,".")==0 || strcmp(de->d_name,"..")==0) continue;
            // anything exists => EEXIST
            closedir(d);
            return -ENOTEMPTY;
        }
        closedir(d);
    }
    if (rmdir(md) != 0) return -errno;
    prune_empty_parents(md, root);
    return 0;
}

// ------------------------------ init/options ------------------------
static int parse_opt(void *data, const char *arg, int key, struct fuse_args *outargs) {
    (void)data; (void)key; (void)outargs;
    if (strncmp(arg, "-ostorage=", 10) == 0) {
        strncpy(g_storage, arg + 10, sizeof(g_storage)-1);
        return 0;
    }
    if (strncmp(arg, "storage=", 8) == 0) {
        strncpy(g_storage, arg + 8, sizeof(g_storage)-1);
        return 0;
    }
    return 1; // keep
}

// ---------- ATTR OPS: chmod/chown/utimens/access -----------
// FUSE3: chmod(path, mode, fi)
static int borgfs_chmod(const char *path, mode_t mode, struct fuse_file_info *fi){
    (void)fi;
    char mp[PATH_MAX]; path_meta(relpath(path), mp);
    if (chmod(mp, mode) == -1) return -errno;
    return 0;
}

// FUSE3: chown(path, uid, gid, fi)
static int borgfs_chown(const char *path, uid_t uid, gid_t gid, struct fuse_file_info *fi){
    (void)fi;
    char mp[PATH_MAX]; path_meta(relpath(path), mp);
    if (lchown(mp, uid, gid) == -1) return -errno;  // lchown: nie podąża za symlinkiem
    return 0;
}

// FUSE3: utimens(path, tv, fi)
static int borgfs_utimens(const char *path, const struct timespec tv[2], struct fuse_file_info *fi){
    (void)fi;
    char mp[PATH_MAX]; path_meta(relpath(path), mp);
    if (utimensat(AT_FDCWD, mp, tv, 0) == -1) return -errno;
    return 0;
}


static int borgfs_access(const char *path, int mask)
{
    (void)mask;
    // optionally you can check permissions on meta:
    char mp[PATH_MAX];
    path_meta(relpath(path), mp);
    if (access(mp, F_OK) == 0) return 0;
    // if meta doesn't exist but the file logically exists, you can return 0; keeping it simple:
    return -errno;
}




static void *borgfs_init(struct fuse_conn_info *conn, struct fuse_config *cfg){
    (void)conn;
    cfg->kernel_cache = 1;
    cfg->attr_timeout = 5;
    cfg->entry_timeout= 5;
    cfg->negative_timeout = 2;
    // Default Setting
    cfg->auto_cache       = 1;   // equivalent of -o auto_cache
 
    // Increased readahead (only via fuse_conn_info, because fuse_config doesn't have it)
    // cfg->max_readahead = 16 * 1024 * 1024;

    // (in some libfuse3 versions, the field in conn is also honored)
    conn->max_readahead = 16 * 1024 * 1024;

    // — capabilities: request forwarding of flocks/posix locks —
    conn->want |= FUSE_CAP_FLOCK_LOCKS;   // flock(2) → .flock
    conn->want |= FUSE_CAP_POSIX_LOCKS;   // fcntl()  → .lock (if implemented)

    // (optional) writeback cache – only if handled in the FS:
    // conn->want |= FUSE_CAP_WRITEBACK_CACHE;

    // auxiliary log: check what the kernel actually accepts
    // fprintf(stderr, "capable=%llx want=%llx\n",
    //         (unsigned long long)conn->capable,
    //         (unsigned long long)conn->want);

    ensure_dirs();
    return NULL;
}

static struct fuse_operations ops = {
    .init       = borgfs_init,
    .getattr    = borgfs_getattr,
    .readdir    = borgfs_readdir,
    .open       = borgfs_open,
    .read       = borgfs_read,
    .write      = borgfs_write,
    .flush      = borgfs_flush,
    .fsync      = borgfs_fsync,
    .release    = borgfs_release,
    .create     = borgfs_create,
    .truncate   = borgfs_truncate,
    .statfs     = borgfs_statfs,
    .unlink     = borgfs_unlink,
    .mkdir      = borgfs_mkdir,
    .rmdir      = borgfs_rmdir,
    .flock	= borgfs_flock,
    .destroy	= borgfs_destroy,
    .chmod      = borgfs_chmod,
    .chown	= borgfs_chown,
    .utimens	= borgfs_utimens,
    //.access	= borgfs_access,
};

int main(int argc, char *argv[]){
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
    // extract -o storage=... both as "-ostorage=..." and "storage=..."
    fuse_opt_parse(&args, NULL, NULL, parse_opt);
    if (g_storage[0]=='\0'){
        fprintf(stderr, "Usage: %s -o storage=/path/to/backend <mountpoint>\\n", argv[0]);
        return 1;
    }
    if (ensure_dirs()!=0){ perror("ensure_dirs"); return 1; }

    return fuse_main(args.argc, args.argv, &ops, NULL);
}
