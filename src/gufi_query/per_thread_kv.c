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

#include "gufi_query/per_thread_kv.h"
#include "str.h"
#include "utils.h"

#define PER_THREAD_KV "temp.PER_THREAD_KV"
#define KEY           "key"
#define VALUE         "value"

int ptkv_init(sqlite3 *db) {
    int rc = SQLITE_OK;
    char *err = NULL;

    /* create the per-thread kv table */
    rc = sqlite3_exec(db,
                      "DROP TABLE IF EXISTS " PER_THREAD_KV ";"
                      "CREATE TABLE " PER_THREAD_KV "(" KEY " TEXT PRIMARY KEY, " VALUE " TEXT);",
                      NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Error: Could not create per-thread kv table: %s\n", err);
        return rc;
    }

    /* add per-thread kv store setter */
    rc = sqlite3_create_function(db, "ptkv_set", 2, SQLITE_UTF8,
                                 db, &ptkv_set, NULL, NULL);
    if (rc != SQLITE_OK) {
        sqlite_print_err_and_free(NULL, stderr, "Error: Could not create pktv_set function\n");
        return rc;
    }

    /* add per-thread kv store getter */
    rc = sqlite3_create_function(db, "ptkv_get", 1, SQLITE_UTF8,
                                 db, &ptkv_get, NULL, NULL);
    if (rc != SQLITE_OK) {
        sqlite_print_err_and_free(NULL, stderr, "Error: Could not create pktv_get function\n");
        return rc;
    }

    return SQLITE_OK;
}

/* ptkv_set(key, value) */
void ptkv_set(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;

    sqlite3 *db = (sqlite3 *) sqlite3_user_data(context);
    const char  *key     = (const char *) sqlite3_value_text(argv[0]);
    const size_t key_len = strlen(key);
    const char  *val     = (const char *) sqlite3_value_text(argv[1]);
    const size_t val_len = strlen(val);

    #define DELETE "DELETE FROM " PER_THREAD_KV " WHERE " KEY " == "
    #define INSERT "INSERT INTO " PER_THREAD_KV "(" KEY ", " VALUE ") VALUES ("

    const size_t sql_len = (
        ((sizeof(DELETE) - 1) +
         1 + key_len + 1 +         /* 'key' */
         1 + 1) +                  /* ; + space */
        ((sizeof(INSERT) - 1) +
         1 + key_len + 1 +         /* 'key' */
         1 + 1 +                   /* , + space */
         1 + val_len + 1 +         /* 'value' */
         1 + 1)                    /* ); */
        );

    char *sql = malloc(sql_len + 1);
    SNFORMAT_S(sql, sql_len + 1, 10,
               DELETE, sizeof(DELETE) - 1,
               "'",    (size_t) 1,
               key,    key_len,
               "'; ",  (size_t) 3,
               INSERT, sizeof(INSERT) - 1,
               "'",    (size_t) 1,
               key,    key_len,
               "', '", (size_t) 4,
               val,    val_len,
               "');",  (size_t) 3);

    char *err = NULL;
    const int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    free(sql);

    if (rc != SQLITE_OK) {
        sqlite3_result_error(context, err, -1);
        sqlite3_free(err);
    }
}

int ptkv_set_internal(const size_t tid, sqlite3 *db,
                      const char *key, const size_t key_len,
                      const refstr_t *val) {
    #define PTKV_SET "SELECT ptkv_set("
    const size_t sql_len = (
        (sizeof(PTKV_SET) - 1) +
        1 + key_len + 1 +   /* 'key' */
        1 + 1 +             /* , + space */
        1 + val->len + 1 +  /* 'value' */
        1 + 1               /* ); */
        );

    char *sql = malloc(sql_len + 1);
    SNFORMAT_S(sql, sql_len + 1, 6,
               PTKV_SET, (sizeof(PTKV_SET) - 1),
               "'",    (size_t) 1,
               key, key_len,
               "', '", (size_t) 4,
               val->data, val->len,
               "');",  (size_t) 3);

    char *err = NULL;
    const int rc = sqlite3_exec(db, sql, NULL, NULL, &err);
    free(sql);
    if (rc != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Error: Could not set predefined key \"%s\" on thread %zu: %s\n",
                                  key, tid, err);
        return rc;
    }

    return SQLITE_OK;
}

static int ptkv_get_val(void *args, int count, char **data, char **columns) {
    (void) count; (void) columns;

    str_t *str = (str_t *) args;
    str->len = strlen(data[0]);
    str->data = malloc(str->len + 1);
    memcpy(str->data, data[0], str->len);
    str->data[str->len] = '\0';
    return 0;
}

/* ptkv_get(key) */
void ptkv_get(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void) argc;

    sqlite3 *db = (sqlite3 *) sqlite3_user_data(context);
    const char  *key     = (const char *) sqlite3_value_text(argv[0]);
    const size_t key_len = strlen(key);

    #define SELECT "SELECT " VALUE " FROM " PER_THREAD_KV " WHERE " KEY " == "

    const size_t sql_len = (
        (sizeof(SELECT) - 1) +
        1 + key_len + 1 +    /* 'key' */
        1                    /* ; */
        );

    char *sql = malloc(sql_len + 1);
    SNFORMAT_S(sql, sql_len + 1, 4,
               SELECT, sizeof(SELECT) - 1,
               "'", (size_t) 1,
               key, key_len,
               "';", (size_t) 2);

    str_t val;
    char *err = NULL;
    const int rc = sqlite3_exec(db, sql, ptkv_get_val, &val, &err);
    free(sql);

    if (rc != SQLITE_OK) {
        sqlite3_result_error(context, err, -1);
        sqlite3_free(err);
    }

    sqlite3_result_text(context, val.data, val.len, SQLITE_TRANSIENT);
    str_free_existing(&val);
}

void ptkv_fini(sqlite3 *db) {
    /* dropping PER_THREAD_KV is not strictly necessary since it is in the temp namespace */
    char *err = NULL;
    if (sqlite3_exec(db, "DROP TABLE IF EXISTS " PER_THREAD_KV ";", NULL, NULL, &err) != SQLITE_OK) {
        sqlite_print_err_and_free(err, stderr, "Error: Could not drop per-thread kv table: %s\n", err);
    }
}
