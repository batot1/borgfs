// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 Bartosz Bartczak
//
// This is a benchmark file for testing algorithm speed.
// tests/bench_crc_blake.c
// Version 2

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <x86intrin.h>
#include "blake3.h"

static inline uint64_t now_ns(){ struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec*1000000000ull + ts.tv_nsec; }

static inline uint64_t crc32c_u64(const uint8_t* p, size_t n){
  uint64_t crc=0; size_t i=0;
  for(; i+8<=n; i+=8) crc = _mm_crc32_u64(crc, *(const uint64_t*)(p+i));
  for(; i<n; ++i) crc = _mm_crc32_u8((uint32_t)crc, p[i]);
  return crc;
}

int main(){
  const size_t N = (size_t)1<<28; // 256 MiB
  uint8_t *buf = aligned_alloc(64, N);
  for (size_t i=0;i<N;i++) buf[i] = (uint8_t)rand();

  // warmup
  (void)crc32c_u64(buf, N);
  uint8_t out[32]; blake3_hasher h; blake3_hasher_init(&h);
  blake3_hasher_update(&h, buf, N); blake3_hasher_finalize(&h,out,32);

  // CRC
  uint64_t t0=now_ns(); volatile uint64_t c=0;
  for(int i=0;i<8;i++) c ^= crc32c_u64(buf, N);
  uint64_t t1=now_ns();

  // BLAKE3
  uint64_t t2=now_ns();
  for(int i=0;i<8;i++){ blake3_hasher_init(&h); blake3_hasher_update(&h, buf, N);
                        blake3_hasher_finalize(&h,out,32); }
  uint64_t t3=now_ns();

  double crc_gib = (double)N*8 / (1<<30);
  double b3_gib  = (double)N*8 / (1<<30);
  double crc_gibs= crc_gib / ((t1-t0)/1e9);
  double b3_gibs = b3_gib  / ((t3-t2)/1e9);
  printf("CRC32C: %.2f GiB/s, BLAKE3: %.2f GiB/s (N=%.0f MiB)\n",
         crc_gibs, b3_gibs, (double)N/(1<<20));
  printf("crc sink=%llu  b3 sink=%02x\n", (unsigned long long)c, out[0]);
  free(buf); return 0;
}

