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



#include <cstdio>

#include <gtest/gtest.h>

#include "plugin.h"

TEST(plugin, bad_arg) {
    char name[1024] = {};
    std::size_t len = 0;

    len = 10;
    EXPECT_EQ(load_plugin_library(nullptr, len), nullptr);

    len = 0;
    EXPECT_EQ(load_plugin_library(name,    len), nullptr);

    len = snprintf(name, sizeof(name), ":"); // ":filename" would cause "filename" to be read as the entrypoint
    EXPECT_EQ(load_plugin_library(name,    len), nullptr);

    len = snprintf(name, sizeof(name), "entrypoint:");
    EXPECT_EQ(load_plugin_library(name,    len), nullptr);
}

// thread count cannot be 0
static const std::size_t THREADS = 2;

static int test_global_init(struct input *in) {
    return !!in;
}

/* What thread_init hands out, and what the later hooks were handed back.
   Static rather than captured so the plain C function pointers can reach
   them -- this is what proves per-thread state survives from thread_init
   into ctx_init, which is the point of the hook. */
static std::size_t test_thread_state = 0;
static void *test_ctx_saw_thread_data = nullptr;
static void *test_thread_exit_saw_thread_data = nullptr;

static int test_thread_init(sqlite3 *db, void **thread_data) {
    *thread_data = &test_thread_state;
    return !!db;
}

static void *test_ctx_init(void *ptr, void *thread_data) {
    test_ctx_saw_thread_data = thread_data;
    return ptr; /* ptr and user_data are the same */
}

static void test_pre_process_dir(void *, void *user_data) {
    std::size_t *value = static_cast<std::size_t *>(user_data);
    (*value)++;
}

static plugin_file_action test_pre_process_file(void *, void *user_data) {
    std::size_t *value = static_cast<std::size_t *>(user_data);
    (*value)++;
    return PLUGIN_PROCESS_FILE;
}

static void test_post_process_dir(void *, void *user_data) {
    std::size_t *value = static_cast<std::size_t *>(user_data);
    (*value)++;
}

static void test_post_process_file(void *, void *user_data) {
    std::size_t *value = static_cast<std::size_t *>(user_data);
    (*value)++;
}

static void test_ctx_exit(void *, void *) {}

static void test_thread_exit(sqlite3 *, void *thread_data) {
    test_thread_exit_saw_thread_data = thread_data;
}

static void test_global_exit(struct input *) {}

TEST(plugins, good) {
    // 0 plugins
    {
        struct plugins plugins = {};
        EXPECT_EQ(plugins_init(&plugins, 0, THREADS), &plugins);
        plugins_destroy(&plugins);
    }

    static const plugin_type TEST_PLUGIN_TYPE = (plugin_type) 4;

    struct plugin_operations full_ops;
    full_ops.type              = TEST_PLUGIN_TYPE;
    full_ops.global_init       = test_global_init;
    full_ops.thread_init       = test_thread_init;
    full_ops.ctx_init          = test_ctx_init;
    full_ops.pre_process_dir   = test_pre_process_dir;
    full_ops.pre_process_file  = test_pre_process_file;
    full_ops.post_process_dir  = test_post_process_dir;
    full_ops.post_process_file = test_post_process_file;
    full_ops.ctx_exit          = test_ctx_exit;
    full_ops.thread_exit       = test_thread_exit;
    full_ops.global_exit       = test_global_exit;

    struct plugin *full_plugin = (struct plugin *) malloc(sizeof(*full_plugin));
    full_plugin->filename      = "full plugin";
    full_plugin->handle        = nullptr;
    full_plugin->ops           = &full_ops;

    struct plugin_operations no_ops;
    memset(&no_ops, 0, sizeof(no_ops));
    no_ops.type                = TEST_PLUGIN_TYPE;

    struct plugin *no_plugin   = (struct plugin *) malloc(sizeof(*no_plugin));
    no_plugin->filename        = "no plugin";
    no_plugin->handle          = nullptr;
    no_plugin->ops             = &no_ops;

    const std::size_t count = 2;

    struct plugins plugins = {};
    EXPECT_EQ(plugins_init(&plugins, count, THREADS), &plugins);
    plugins.plugins[0] = full_plugin;
    plugins.plugins[1] = no_plugin;

    EXPECT_EQ(plugins_check_type(&plugins, (plugin_type) 4), count);

    // fail
    EXPECT_EQ(plugins_global_init(&plugins, reinterpret_cast<struct input *>(&plugins)), (std::size_t) 0);
    EXPECT_EQ(plugins_thread_init(&plugins, reinterpret_cast<sqlite3 *>  (&plugins), 0), (std::size_t) 0);
    /* the failing plugin's state must not be kept */
    EXPECT_EQ(plugins.thread_data[0][0], nullptr);

    // ctx
    std::size_t value = 0;

    // success
    EXPECT_EQ(plugins_global_init(&plugins, nullptr),  count);
    plugins_thread_init      (&plugins, nullptr, 0);
    /* thread_init's out-param is kept per (thread, plugin) ... */
    EXPECT_EQ(plugins.thread_data[0][0], &test_thread_state);
    plugins_ctx_init         (&plugins, &value,  0);
    /* ... and handed to ctx_init, which runs once per DIRECTORY and so
       cannot hold anything itself. */
    EXPECT_EQ(test_ctx_saw_thread_data, &test_thread_state);
    plugins_pre_process_dir  (&plugins, nullptr, 0); // nullptr should be &value, but intentially not passing in
    plugins_pre_process_file (&plugins, nullptr, 0); // nullptr should be &value, but intentially not passing in
    plugins_post_process_dir (&plugins, nullptr, 0); // nullptr should be &value, but intentially not passing in
    plugins_post_process_file(&plugins, nullptr, 0); // nullptr should be &value, but intentially not passing in
    plugins_ctx_exit         (&plugins, nullptr, 0); // nullptr should be &value, but intentially not passing in
    plugins_thread_exit      (&plugins, nullptr, 0);
    /* and handed back on the way out, so the plugin can free it */
    EXPECT_EQ(test_thread_exit_saw_thread_data, &test_thread_state);
    EXPECT_EQ(plugins.thread_data[0][0], nullptr);
    plugins_global_exit      (&plugins, nullptr);
    EXPECT_EQ(value, (std::size_t) 4);

    plugins_destroy(&plugins);
}

TEST(plugins, bad) {
    struct plugins plugins = {};

    // bad plugin
    EXPECT_EQ(plugins_init(nullptr,  (std::size_t)  0, THREADS),         nullptr);

    // bad plugin count
    EXPECT_EQ(plugins_init(&plugins, (std::size_t) -1, THREADS),         nullptr);

    // bad thread count
    EXPECT_EQ(plugins_init(&plugins, (std::size_t) 2, (std::size_t)  0), nullptr);
    EXPECT_EQ(plugins_init(&plugins, (std::size_t) 2, (std::size_t) -1), nullptr);
}
