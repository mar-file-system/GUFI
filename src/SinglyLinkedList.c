/*
This file is part of GUFI, which is part of MarFS, which is released
under the BSD license.


Copyright (c) 2017, Los Alamos National Security (LANS), LLC
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation and/or
other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors
may be used to endorse or promote products derived from this software without
specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


From Los Alamos National Security, LLC:
LA-CC-15-039

Copyright (c) 2017, Los Alamos National Security, LLC All rights reserved.
Copyright 2017. Los Alamos National Security, LLC. This software was produced
under U.S. Government contract DE-AC52-06NA25396 for Los Alamos National
Laboratory (LANL), which is operated by Los Alamos National Security, LLC for
the U.S. Department of Energy. The U.S. Government has rights to use,
reproduce, and distribute this software.  NEITHER THE GOVERNMENT NOR LOS
ALAMOS NATIONAL SECURITY, LLC MAKES ANY WARRANTY, EXPRESS OR IMPLIED, OR
ASSUMES ANY LIABILITY FOR THE USE OF THIS SOFTWARE.  If software is
modified to produce derivative works, such modified software should be
clearly marked, so as not to confuse it with the version available from
LANL.

THIS SOFTWARE IS PROVIDED BY LOS ALAMOS NATIONAL SECURITY, LLC AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL LOS ALAMOS NATIONAL SECURITY, LLC OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
OF SUCH DAMAGE.
*/



#include <stdlib.h>
#include <string.h>

#include "SinglyLinkedList.h"

struct SinglyLinkedListNode {
    void *data;
    struct SinglyLinkedListNode *next;
};

static sll_t *sll_clear(sll_t *sll) {
    /* if (!sll) { */
    /*     return NULL; */
    /* } */
    return memset(sll, 0, sizeof(sll_t));
}

sll_t *sll_init(sll_t *sll) {
    return sll_clear(sll);
};

sll_t *sll_push(sll_t *sll, void *data) {
    /* if (!sll) { */
    /*     return NULL; */
    /* } */

    sll_node_t *node = calloc(1, sizeof(sll_node_t));
    node->data = data;

    if (!sll->head) {
        sll->head = node;
    }

    if (sll->tail) {
        sll->tail->next = node;
    }

    sll->tail = node;

    sll->size++;

    return sll;
}

sll_t *sll_move(sll_t *dst, sll_t *src) {
    /* if (!dst || !src) { */
    /*     return NULL; */
    /* } */

    /* dst is overwritten, not destroyed */
    *dst = *src;
    sll_clear(src);
    return dst;
}

sll_t *sll_move_append(sll_t *dst, sll_t *src) {
    /* if (!dst || !src) { */
    /*     return NULL; */
    /* } */

    /* src is appended to dst and then cleared */
    if (!dst->head) {
        dst->head = src->head;
    }

    if (dst->tail) {
        dst->tail->next = src->head;
    }

    if (src->tail) {
        dst->tail = src->tail;
    }

    dst->size += src->size;

    sll_clear(src);
    return dst;
}

size_t sll_get_size(sll_t *sll) {
    return sll?sll->size:0;
}

sll_node_t *sll_head_node(sll_t *sll) {
    return sll?sll->head:NULL;
}

sll_node_t *sll_next_node(sll_node_t *node) {
    return node?node->next:NULL;
}

void *sll_node_data(sll_node_t *node) {
    return node?node->data:NULL;
}

void sll_destroy(sll_t *sll, void (*destroy)(void *)) {
    sll_node_t *node = sll_head_node(sll);
    while (node) {
        if (destroy) {
            destroy(sll_node_data(node));
        }
        sll_node_t *next = sll_next_node(node);
        free(node);
        node = next;
    }

    sll_clear(sll);
}
