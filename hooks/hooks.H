#ifndef __HOOKS_H__
#define __HOOKS_H__

#include "libdft_api.h"

/* dtracker_openclose.cpp */
void pre_open_hook(syscall_ctx_t *ctx);
void post_open_hook(syscall_ctx_t *ctx);
void post_close_hook(syscall_ctx_t *ctx);

/* dtracker_read.cpp */
void post_read_hook(syscall_ctx_t *ctx);
void post_readv_hook(syscall_ctx_t *ctx);

/* dtracker_write.cpp */
void post_write_hook(syscall_ctx_t *ctx);
void post_writev_hook(syscall_ctx_t *ctx);

/* dtracker_mmap.cpp */
void post_mmap2_hook(syscall_ctx_t *ctx);
void post_munmap_hook(syscall_ctx_t *ctx);
void post_mremap_hook(syscall_ctx_t *ctx);

#endif
