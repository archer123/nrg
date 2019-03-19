#ifndef __BSPC_H__
#define __BSPC_H__

#include "list.h"
#include "hashtable.h"
#include "nso_common.h"
#include "log.h"
#include <stdint.h>
#include <pthread.h>
#include "nso_if.h"
#include <stdlib.h>

typedef struct {
    uint16_t proto;
    int sockfd;
    struct list_head ll;
    struct hlist_node hl;
}bspc_entry_t;

static bspc_entry_t* alloc_bspc_entry(uint16_t proto, int sockfd) {
    bspc_entry_t *e = malloc(sizeof(bspc_entry_t));
    if (!e) {
        LOG_DEBUG("bspc entry allocation failed\n");
        return NULL;
    }
    memset(e, 0, sizeof(bspc_entry_t));
    e->proto = proto;
    e->sockfd = sockfd;
    return e;
}

static void free_bspc_entry(bspc_entry_t *e) {
    if(e)
        free(e);
}

//1024 buckets
#define BSPC_HASH_BITS 10

struct bspc_table_s {
    int size;
    struct list_head ll_head;
    DECLARE_HASHTABLE(ht_proto, BSPC_HASH_BITS);
};

typedef struct bspc_table_s bspc_table_t;

bspc_table_t* bspc_table_create();
void bspc_table_init(bspc_table_t *tbl);

int bspc_table_add(bspc_table_t *tbl, bspc_entry_t *e);

bspc_entry_t* bspc_table_lookup(bspc_table_t *tbl, uint16_t proto);

int bspc_table_del(bspc_table_t *tbl, bspc_entry_t *e);

//just destroy
void bspc_table_destroy(bspc_table_t *tbl);

//destroy and free allocated memory of tbl
void bspc_table_free(bspc_table_t *tbl);


#endif
