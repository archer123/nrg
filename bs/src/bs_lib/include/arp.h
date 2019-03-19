#ifndef __ARP_H__
#define __ARP_H__

#include "list.h"
#include "nso_common.h"
#include "hashtable.h"
#include "log.h"
#include <stdint.h>
#include <pthread.h>

typedef enum {
    ARP_ACTIVE = 0,
    ARP_EXPIRED = 1,
}arp_entry_status_e;

typedef struct {
    device_id_t *dev_id;
    l2addr_t *l2addr;

    struct list_head ll;
    struct hlist_node hl_devid;
    struct hlist_node hl_l2addr;

    arp_entry_status_e status;

}arp_entry_t;

static arp_entry_t* alloc_arp_entry(device_id_t *dev_id, l2addr_t *l2addr) {
    arp_entry_t *e = malloc(sizeof(arp_entry_t));
    if (!e) {
        LOG_DEBUG("arp entry alloc failed\n");
        return NULL;
    }
    memset(e, 0, sizeof(arp_entry_t));
    e->dev_id = dev_id;
    e->l2addr = l2addr;
    e->status = ARP_ACTIVE;
    return e;
}

static void free_arp_entry(arp_entry_t *e) {
    if (e) {
        free_device_id(e->dev_id);
        free_l2addr(e->l2addr);
        free(e);
    }
}

//max number of bucket is 1024
#define ARPT_HASH_BITS 10

struct arp_table_s {
    int size;
    //used to store (device_id, l2addr) tuple in a linear list
    struct list_head ll_head;
    //hash table which uses device_id as the key
    DECLARE_HASHTABLE(ht_dev_id, ARPT_HASH_BITS);
    //hash table which uses l2addr as the key
    DECLARE_HASHTABLE(ht_l2addr, ARPT_HASH_BITS);
    //lock?
    pthread_mutex_t arpt_lock;
};

typedef struct arp_table_s arp_table_t;

//operation
//create
arp_table_t* arp_table_create();
void arp_table_init(arp_table_t*);
//add new entry
int arp_table_add(arp_table_t*, arp_entry_t*);
//lookup entry from dev_id
arp_entry_t* arp_table_lookup_from_dev_id(arp_table_t*, device_id_t*);
//lookup entry from l2addr
arp_entry_t* arp_table_lookup_from_l2addr(arp_table_t*, l2addr_t*);
//del entry
int arp_table_del(arp_table_t*, arp_entry_t*);
//aging
int arp_table_aging(arp_table_t*);
//destroy
void arp_table_destroy(arp_table_t*);
//destroy & free memory of tbl allocated by malloc
void arp_table_free(arp_table_t *tbl);

static void arp_table_lock(arp_table_t *arpt) {
    pthread_mutex_lock(&arpt->arpt_lock);
}

static void arp_table_unlock(arp_table_t *arpt) {
    pthread_mutex_unlock(&arpt->arpt_lock);
}

static arp_entry_t* arp_table_lookup_from_dev_id_unsafe(arp_table_t *tbl, device_id_t *dev_id) {
    arp_entry_t *pos = NULL;
    hash_for_each_possible(tbl->ht_dev_id, pos, hl_devid, hash_device_id(dev_id)) {
        if (device_id_equal(dev_id, pos->dev_id))
            break;
    }
    return pos;
}

static arp_entry_t* arp_table_lookup_from_l2addr_unsafe(arp_table_t *tbl, l2addr_t *addr) {
    arp_entry_t *pos = NULL;
    hash_for_each_possible(tbl->ht_l2addr, pos, hl_l2addr, hash_l2addr(addr)) {
        if (l2addr_equal(addr, pos->l2addr))
            break;
    }
    return pos;
}

#endif
