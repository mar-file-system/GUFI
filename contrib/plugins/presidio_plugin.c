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



#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cjson/cJSON.h>
#include <curl/curl.h>
#include <sqlite3.h>

#include "SinglyLinkedList.h"
#include "plugin.h"
#include "str.h"

/* defaults */
static const char ANALYZE_URL_DEFAULT[]   = "http://localhost:5002/analyze";
static const char ANONYMIZE_URL_DEFAULT[] = "http://localhost:5001/anonymize";
static const str_t LANG_DEFAULT = {
    .data = "en",
    .len  = 2,
    .free = NULL,
};

/* write results into a buffer instead of to stdout */
static size_t write_callback(void *ptr, size_t size, size_t nmemb, void *userdata) {
    str_t *str = (str_t *) userdata;
    const size_t cp = size * nmemb;
    char *data = realloc(str->data, str->len + cp + 1);
    if (!data) {
        return 0;
    }

    str->data = data;
    memcpy(str->data + str->len, ptr, cp);
    str->data[cp] = '\0';
    str->len += cp;

    return cp;
}

/* do the cURL call */
static int do_curl(CURL *curl, const char *url,
                   const char *json_payload,
                   str_t *response,
                   sqlite3_context *context) {
    curl_easy_setopt(curl, CURLOPT_URL, url);

    struct curl_slist *headers = curl_slist_append(NULL, "Content-Type: application/json");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_payload);

    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, response);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
        sqlite3_result_error(context, curl_easy_strerror(res), -1);
        free(response->data);
        response->data = NULL;
        response->len = 0;
    }

    curl_slist_free_all(headers);

    return (res != CURLE_OK);
}

/*
 * take in raw char * for building payload
 *
 * caller should use analysis
 */
static int presidio_analyzer(CURL *curl, const char *url,
                             const char *text, const size_t text_len,
                             const char *lang, const size_t lang_len,
                             str_t *analysis,
                             sqlite3_context *context) {
    if (!text || !text_len ||
        !lang || !lang_len) {
        memset(analysis, 0, sizeof(*analysis));
        return 0;
    }

    /*
     * JSON Payload
     * {
     *     "text": "<text>",
     *     "language": "%s"
     * }
     */
    const size_t payload_len =
        2 +
        13 + text_len + 2 +
        18 + lang_len + 2 +
        1;
    char *json_payload = malloc(payload_len + 1);
    snprintf(json_payload, payload_len + 1,
             "{\n"
             "    \"text\": \"%s\",\n"
             "    \"language\": \"%s\"\n"
             "}",
             text, lang);

    const int rc = do_curl(curl, url,
                           json_payload,
                           analysis,
                           context);

    free(json_payload);

    return rc;
}

/* presidio_analyze(text[, lang]) -> JSON */
static void presidio_analyze(sqlite3_context *context, int argc, sqlite3_value **argv) {
    CURL *curl = (CURL *) sqlite3_user_data(context);

    const char *text = (char *) sqlite3_value_text(argv[0]);
    const size_t text_len = strlen(text);

    char *lang = LANG_DEFAULT.data;
    size_t lang_len = LANG_DEFAULT.len;
    if (argc > 1) {
        lang = (char *) sqlite3_value_text(argv[1]);
        lang_len = strlen(lang);
    }

    str_t analysis = {0};
    const int rc = presidio_analyzer(curl, ANALYZE_URL_DEFAULT,
                                     text, text_len,
                                     lang, lang_len,
                                     &analysis,
                                     context);
    if (rc == 0) {
        sqlite3_result_text(context, analysis.data, analysis.len, SQLITE_TRANSIENT);
    }
}

/*
 * take in raw char * for building payload
 *
 * caller should use anonymized
 */
static int presidio_anonymizer(CURL *curl, const char *url,
                               const char *text, const size_t text_len,
                               const str_t *analysis,
                               str_t *anonymized,
                               sqlite3_context *context) {
    if (!text || !text_len ||
        !analysis->data|| !analysis->len) {
        memset(anonymized, 0, sizeof(*anonymized));
        return 0;
    }

    /*
     * JSON Payload
     * {
     *     "text": "<text>",
     *     "analyzer_results": <analysis>
     * }
     */
    const size_t json_payload_len = 2 +
        13 + text_len + 3 +
        24 + analysis->len + 1 +
        1;

    char *json_payload = malloc(json_payload_len + 1);
    snprintf(json_payload, json_payload_len + 1,
             "{\n"
             "    \"text\": \"%s\",\n"
             "    \"analyzer_results\": %s\n"
             "}",
             text, analysis->data);

    const int rc = do_curl(curl, url,
                           json_payload,
                           anonymized,
                           context);

    free(json_payload);

    return rc;
}

/* presidio_anonymize(text[, lang]) -> anonymized text */
static void presidio_anonymize(sqlite3_context *context, int argc, sqlite3_value **argv) {
    CURL *curl = (CURL *) sqlite3_user_data(context);
    (void) argc;

    char *text = (char *) sqlite3_value_text(argv[0]);
    if (!text) {
        sqlite3_result_null(context);
        return;
    }

    const size_t text_len = strlen(text);
    if (text_len == 0) {
        sqlite3_result_null(context);
        return;
    }

    char *lang = LANG_DEFAULT.data;
    size_t lang_len = LANG_DEFAULT.len;
    if (argc > 1) {
        lang = (char *) sqlite3_value_text(argv[1]);
        lang_len = strlen(lang);
    }

    str_t analysis_response = {0};
    if (presidio_analyzer(curl, ANALYZE_URL_DEFAULT,
                          text, text_len,
                          lang, lang_len,
                          &analysis_response,
                          context) != 0) {
        /* actual error would already have been returned */
        return;
    }

    str_t anonymized_response = {0};
    const int rc = presidio_anonymizer(curl, ANONYMIZE_URL_DEFAULT,
                                       text, text_len,
                                       &analysis_response,
                                       &anonymized_response,
                                       context);

    free(analysis_response.data);

    if (rc != 0) {
        /* actual error would already have been returned */
        return;
    }

    /* extract anonymized text from returned JSON */

    cJSON *json = cJSON_Parse(anonymized_response.data);
    if (!json) {
        sqlite3_result_error(context, "cJSON_Parse failed to parse response", -1);
        goto cleanup;
    }

    cJSON *anon_text = cJSON_GetObjectItem(json, "text");
    if (!anon_text) {
        sqlite3_result_error(context, "Could not get anonymized text from response", -1);
        goto cleanup;
    }

    if (!cJSON_IsString(anon_text) || !anon_text->valuestring) {
        sqlite3_result_error(context, "text field is not a string", -1);
        cJSON_Delete(json);
        goto cleanup;
    }

    sqlite3_result_text(context, anon_text->valuestring, -1, SQLITE_TRANSIENT);

    cJSON_Delete(json);

  cleanup:
    free(anonymized_response.data);
}

static sll_t curl_instances; /* CURL * */
static int loaded = 0;

static int presidio_global_init(struct input *in) {
    (void) in;

    if (loaded) {
        fprintf(stderr, "Error: Cannot load presidio plugin multiple times\n");
        return 1;
    }
    loaded++;

    curl_global_init(CURL_GLOBAL_ALL);
    sqlite3_initialize();
    sll_init(&curl_instances);

    return 0;
}

static int presidio_thread_init(sqlite3 *db) {
    CURL *curl = curl_easy_init();
    if (!curl) {
        fprintf(stderr, "curl_easy_init() failed\n");
        return 1;
    }

    if ((sqlite3_create_function(db,   "presidio_analyze",     -1, SQLITE_UTF8,
                                 curl, &presidio_analyze,      NULL, NULL) != SQLITE_OK) ||
        (sqlite3_create_function(db,   "presidio_anonymize",   -1, SQLITE_UTF8,
                                 curl, &presidio_anonymize,    NULL, NULL) != SQLITE_OK)) {
        fprintf(stderr, "Error: Could not create presidio plugin functions\n");
        curl_easy_cleanup(curl);

        return 1;
    }

    /*
     * cannot pass CURL * to sqlite3_create_function_v2 xDestroy
     * because by the time it is called, the cURL library will
     * have been cleaned up, so keep track of it here
     */
    sll_push_back(&curl_instances, curl);
    return 0;
}

static void curl_instance_free(void *ptr) {
    curl_easy_cleanup(ptr);
}

static void presidio_global_exit(struct input *in) {
    (void) in;

    sll_destroy(&curl_instances, curl_instance_free);
    sqlite3_shutdown();
    curl_global_cleanup();

    loaded--;
}

struct plugin_operations presidio = {
    .type = PLUGIN_QUERY,
    .global_init = presidio_global_init,
    .thread_init = presidio_thread_init,
    .dir_action = NULL,
    .ctx_init = NULL,
    .process_dir = NULL,
    .process_file = NULL,
    .ctx_exit = NULL,
    .thread_exit = NULL,
    .global_exit = presidio_global_exit,
};
