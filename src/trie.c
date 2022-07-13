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



/* Adapted to character representation of long long from                  */
/* http://www.techiedelight.com/trie-implementation-insert-search-delete/ */
#include <stdint.h>
#include <stdlib.h>

#include "trie.h"

// Function that returns a new Trie node
trie_t *trie_alloc()
{
    trie_t* node = (trie_t*)malloc(sizeof(trie_t));
    node->isLeaf = 0;

    for (int i = 0; i < 256; i++)
        node->character[i] = NULL;

    return node;
}

// Iterative function to insert a string in Trie.
void trie_insert(trie_t *head, const char *str, const size_t len)
{
    if (!head || !str) {
        return;
    }

    // start from root node
    trie_t *curr = head;
    for(size_t i = 0; i < len; i++) {
        const uint8_t c = str[i];

        // create a new node if path doesn't exists
        if (curr->character[c] == NULL)
            curr->character[c] = trie_alloc();

        // go to next node
        curr = curr->character[c];
    }

    // mark current node as leaf
    curr->isLeaf = 1;
}

// Iterative function to search a string in Trie. It returns 1
// if the string is found in the Trie, else it returns 0
int trie_search(trie_t *head, const char *str, const size_t len)
{
    // return 0 if Trie is empty
    if (head == NULL)
        return 0;

    if (str == NULL) {
        return 0;
    }

    trie_t *curr = head;
    for(size_t i = 0; i < len; i++) {
        const uint8_t c = str[i];

        // go to next node
        curr = curr->character[c];

        // if string is invalid (reached end of path in Trie)
        if (curr == NULL)
            return 0;
    }

    // if current node is a leaf and we have reached the
    // end of the string, return 1
    return curr->isLeaf;
}

// returns 1 if given node has any children
int trie_have_children(trie_t *curr)
{
    for (int i = 0; i < 256; i++)
        if (curr->character[i])
            return 1;    // child found

    return 0;
}

// Recursive function to delete a string in Trie.
int trie_delete_recursive(trie_t **curr, const char *str, const size_t i, const size_t len)
{
    // return if Trie is empty
    if (*curr == NULL)
        return 0;

    // if we have not reached the end of the string
    if (i < len)
    {
        const uint8_t c = str[i];

        // recurse for the node corresponding to next character in
        // the string and if it returns 1, delete current node
        // (if it is non-leaf)
        if (*curr != NULL && (*curr)->character[c] != NULL &&
            trie_delete_recursive(&((*curr)->character[c]), str, i + 1, len) &&
            (*curr)->isLeaf == 0)
        {
            if (!trie_have_children(*curr))
            {
                free(*curr);
                (*curr) = NULL;
                return 1;
            }
            else {
                return 0;
            }
        }
    }

    // if we have reached the end of the string
    if ((i == len) && (*curr)->isLeaf)
    {
        // if current node is a leaf node and don't have any children
        if (!trie_have_children(*curr))
        {
            free(*curr); // delete current node
            (*curr) = NULL;
            return 1; // delete non-leaf parent nodes
        }

        // if current node is a leaf node and have children
        else
        {
            // mark current node as non-leaf node (DON'T DELETE IT)
            (*curr)->isLeaf = 0;
            return 0;       // don't delete its parent nodes
        }
    }

    return 0;
}

int trie_delete(trie_t *head, const char *str, const size_t len) {
    if (!head || !str) {
        return 0;
    }

    if (len == 0) {
        return !!head->isLeaf;
    }

    return trie_delete_recursive(&(head->character[(uint8_t) str[0]]), str, 1, len);
}

void trie_free(trie_t *head) {
    if (head) {
        for(int i = 0; i < 256; i++) {
            trie_free(head->character[i]);
        }

        free(head);
    }
}
