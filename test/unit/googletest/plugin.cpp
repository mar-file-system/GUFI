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
    char name[1024];
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

static int test_global_init(void *global) {
    return !!global;
}

static void *test_ctx_init(void *ptr) {
    return ptr; /* ptr and user_data are the same */
}

static void test_process_dir(void *, void *user_data) {
    std::size_t *value = static_cast<std::size_t *>(user_data);
    (*value)++;
}

static void test_process_file(void *, void *user_data) {
    std::size_t *value = static_cast<std::size_t *>(user_data);
    (*value)++;
}

static void test_ctx_exit(void *, void *) {}

static void test_global_exit(void *) {}

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
    full_ops.ctx_init          = test_ctx_init;
    full_ops.process_dir       = test_process_dir;
    full_ops.process_file      = test_process_file;
    full_ops.ctx_exit          = test_ctx_exit;
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
    EXPECT_EQ(plugins_global_init(&plugins, &plugins), (std::size_t) 0);

    // ctx
    std::size_t value = 0;

    // success
    EXPECT_EQ(plugins_global_init(&plugins, nullptr),  count);
    plugins_ctx_init    (&plugins, &value,  0);
    plugins_process_dir (&plugins, nullptr, 0); // nullptr should be &value, but intentially not passing in
    plugins_process_file(&plugins, nullptr, 0); // nullptr should be &value, but intentially not passing in
    plugins_ctx_exit    (&plugins, nullptr, 0); // nullptr should be &value, but intentially not passing in
    plugins_global_exit (&plugins, nullptr);
    EXPECT_EQ(value, (std::size_t) 2);

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
