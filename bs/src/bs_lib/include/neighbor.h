#ifndef __NEIGHBOR_H__
#define __NEIGHBOR_H__

#include "list.h"
#include "nso_common.h"
#include "hashtable.h"
#include "log.h"
#include <stdint.h>
#include <pthread.h>
#include "nso_if.h"

typedef enum {
    NBR_ACTIVE = 0,
    NBR_EXPIRED = 1,
}nbr_entry_status_e;

typedef struct {
    //For now, we just use some artificial value for the link metric
    int w;
}metric_t;

static metric_t* alloc_metric(int w) {
    metric_t *m = malloc(sizeof(metric_t));
    if (!m) {
        LOG_DEBUG("alloc metric failed!\n");
        return NULL;
    }
    m->w = w;
    return m;
}

static void free_metric(metric_t *m) {
    if (m) free(m);
}


typedef struct {
    device_id_t *dev_id;
    metric_t *metric;
    nso_if_t *iface;

    struct list_head ll;
    struct hlist_node hl_devid;

    nbr_entry_status_e status;

}nbr_entry_t;

static nbr_entry_t* alloc_nbr_entry(device_id_t *dev_id, metric_t *m, nso_if_t *iface) {
    nbr_entry_t *e = malloc(sizeof(nbr_entry_t));
    if (!e) {
        LOG_DEBUG("nbr entry alloc failed\n");
        return NULL;
    }
    memset(e, 0, sizeof(nbr_entry_t));
    e->dev_id = dev_id;
    e->metric = m;
    e->iface = iface;
    e->status = NBR_ACTIVE;
    return e;
}

static void free_nbr_entry(nbr_entry_t *e) {
    if (e) {
        free_device_id(e->dev_id);
        free_metric(e->metric);
        //The *iface pointer is just a reference, should not be freed at here
        free(e);
    }
}

//max number of bucket is 1024
#define NBRT_HASH_BITS 10

struct nbr_table_s {
    int size;
    struct list_head ll_head;
    //hash table which uses device_id as the key
    DECLARE_HASHTABLE(ht_dev_id, NBRT_HASH_BITS);
    //lock?
    pthread_mutex_t nbrt_lock;
};

typedef struct nbr_table_s nbr_table_t;

//operation
//create
nbr_table_t* nbr_table_create();
void nbr_table_init(nbr_table_t*);
//add new entry
int nbr_table_add(nbr_table_t*, nbr_entry_t*);
//lookup entry from dev_id
nbr_entry_t* nbr_table_lookup(nbr_table_t*, device_id_t*);
//del entry
int nbr_table_del(nbr_table_t*, nbr_entry_t*);
//aging
int nbr_table_aging(nbr_table_t*);
//destroy
void nbr_table_destroy(nbr_table_t*);
//destroy & free memory of tbl allocated by malloc
void nbr_table_free(nbr_table_t *tbl);

//shoude be surounded by lock and unlock
int nbr_table_size(nbr_table_t *tbl);

void nbr_table_lock(nbr_table_t *tbl);
void nbr_table_unlock(nbr_table_t *tbl);
nbr_entry_t* nbr_table_lookup_unsafe(nbr_table_t *, device_id_t*);

#define nbr_table_iterate(ptr, tbl) \
    list_for_each_entry(ptr, &tbl->ll_head, ll)

#endif
