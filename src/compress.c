#include <stdlib.h>
#include <string.h>

#include "compress.h"

/* starting offset of actual data */
static const size_t COMP_OFFSET = sizeof(compressed_t);

#if HAVE_ZLIB
#include <zlib.h>

static int compress_zlib(void *dst, size_t *dst_len, void *src, const size_t src_len) {
    return compress(dst, dst_len, src, src_len);
}

static int decompress_zlib(void *dst, size_t *dst_len, void *src, const size_t src_len) {
    return uncompress(dst, dst_len, src, src_len);
}
#endif

/* always return new address */
void *compress_struct(const int comp, void *src, const size_t struct_len) {
    compressed_t *dst = malloc(struct_len);
    dst->yes = 0;
    dst->len = 0;

    if (comp) {
        dst->len = struct_len - COMP_OFFSET;

        #if HAVE_ZLIB
        if (compress_zlib(((unsigned char *) dst) + COMP_OFFSET, &dst->len,
                          ((unsigned char *) src) + COMP_OFFSET,  dst->len) == Z_OK) {
            dst->yes = 1;
            dst->len += COMP_OFFSET;
        }
        #endif

        /* if compressed enough, reallocate to reduce memory usage */
        if (dst->yes && (dst->len < struct_len)) {
            void *r = realloc(dst, COMP_OFFSET + dst->len);
            if (r) {
                dst = r;
            }

            /* if realloc failed, dst can still be used */
            return dst;
        }
    }

    /* if no compresseion or compression failed, copy src */
    memcpy(dst, src, struct_len);

    return dst;
}

int decompress_struct(void **dst, void *src, const size_t struct_len) {
    compressed_t *comp = (compressed_t *) src;
    if (comp->yes) {
        compressed_t *decomp = (compressed_t *) *dst;
        decomp->len = struct_len - COMP_OFFSET;

        #if HAVE_ZLIB
        if (decompress_zlib(((unsigned char *) *dst) + COMP_OFFSET, &decomp->len,
                            ((unsigned char *)  src) + COMP_OFFSET,    comp->len) != Z_OK) {
            return 1;
        }
        #endif

        free(src);
        return 0;
    }

    /*
     * compression not enabled or compression enabled but
     * data was not compressed - use original pointer
     */
    *dst = src;

    return 0;
}

void free_struct(void *used, void *data, const size_t recursion_level) {
    if (used == data) {              /* used received data directly */
        if (recursion_level == 0) {  /* work was not from in-situ call, so data is dynamically allocated */
            free(data);
        }
    }
}
