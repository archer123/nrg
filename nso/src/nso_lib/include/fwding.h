#ifndef __FORWARDING_H__
#define __FORWARDING_H__

#include "list.h"
#include "hashtable.h"
#include "nso_common.h"
#include "log.h"
#include <stdint.h>
#include <pthread.h>
#include "nso_if.h"
#include <stdlib.h>

typedef enum {
    FWD_ACTIVE = 0,
    FWD_EXPIRED = 1,
}fwd_entry_status_e;

typedef struct {
    device_id_t *dest;
    device_id_t *nxthop;
    //memory of interface should not be freed by entry
    //this pointer is just a reference
    nso_if_t *interface;
    fwd_entry_status_e status;

    struct list_head ll;
    struct hlist_node hl;

}fwd_entry_t;

static fwd_entry_t* alloc_fwd_entry(device_id_t *dest, device_id_t *nxthop,
        nso_if_t *interface) {
    assert(dest);
    assert(interface);
    fwd_entry_t *e = malloc(sizeof(fwd_entry_t));
    if (!e) {
        LOG_DEBUG("fwd entry allocation failed\n");
        return NULL;
    }
    memset(e, 0, sizeof(fwd_entry_t));

    e->dest = dest;
    e->interface = interface;
    //if NULL, means dest in the same subnet
    if (!nxthop) {
        nxthop = alloc_device_id((uint8_t*)dest);
    }
    e->nxthop = nxthop;
    e->status = FWD_ACTIVE;
    return e;
}

static void free_fwd_entry(fwd_entry_t *e) {
    assert(e);
    free_device_id(e->dest);
    free_device_id(e->nxthop);
}

//4096 buckets
#define FWDT_HASH_BITS 12
#define FWDT_THREAD_SAFE

struct fwd_table_s {
    int size;
    struct list_head ll_head;
    DECLARE_HASHTABLE(ht_dest, FWDT_HASH_BITS);
#ifdef FWDT_THREAD_SAFE
    pthread_mutex_t fwdt_lock;
#endif
};

typedef struct fwd_table_s fwd_table_t;

fwd_table_t* fwd_table_create();
void fwd_table_init(fwd_table_t *tbl);

int fwd_table_add(fwd_table_t *tbl, fwd_entry_t *e);

fwd_entry_t* fwd_table_lookup(fwd_table_t *tbl, device_id_t *dest);

int fwd_table_del(fwd_table_t *tbl, fwd_entry_t *e);

int fwd_table_aging(fwd_table_t *tbl);

//just destroy
void fwd_table_destroy(fwd_table_t *tbl);

//destroy and free allocated memory of tbl
void fwd_table_free(fwd_table_t *tbl);

static void fwd_table_lock(fwd_table_t *tbl) {
    pthread_mutex_lock(&tbl->fwdt_lock);
}

static void fwd_table_unlock(fwd_table_t *tbl) {
    pthread_mutex_unlock(&tbl->fwdt_lock);
}

static fwd_entry_t* fwd_table_lookup_unsafe(fwd_table_t *tbl, device_id_t *dest) {
    fwd_entry_t *pos = NULL;
    hash_for_each_possible(tbl->ht_dest, pos, hl, hash_device_id(dest)) {
        if (device_id_equal(dest, pos->dest))
            break;
    }
    return pos;
}


#endif
