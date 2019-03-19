#include "fwding.h"


fwd_table_t* fwd_table_create() {
    fwd_table_t *tbl = malloc(sizeof(fwd_table_t));
    if (!tbl) {
        LOG_DEBUG("fwd table create failed!\n");
        return NULL;
    }
    fwd_table_init(tbl);
    return tbl;
}
void fwd_table_init(fwd_table_t *tbl) {
    assert(tbl);
    tbl->size = 0;
    INIT_LIST_HEAD(&tbl->ll_head);
    hash_init(tbl->ht_dest);
#ifdef FWD_THREAD_SAFE
    pthread_mutex_init(&tbl->fwdt_lock, NULL);
#endif
}

int fwd_table_add(fwd_table_t *tbl, fwd_entry_t *e) {
    assert(tbl);
    assert(e);
#ifdef FWD_THREAD_SAFE
    pthread_mutex_lock(&tbl->fwdt_lock);
#endif

    list_add_tail(&e->ll, &tbl->ll_head);
    hash_add(tbl->ht_dest, &e->hl, hash_device_id(e->dest));
    tbl->size++;

#ifdef FWD_THREAD_SAFE
    pthread_mutex_unlock(&tbl->fwdt_lock);
#endif
    return 0;
}

fwd_entry_t* fwd_table_lookup(fwd_table_t *tbl, device_id_t *dest) {
    assert(tbl);
    assert(dest);
#ifdef FWD_THREAD_SAFE
    pthread_mutex_lock(&tbl->fwdt_lock);
#endif
    fwd_entry_t *pos = NULL;
    hash_for_each_possible(tbl->ht_dest, pos, hl, hash_device_id(dest)) {
        if (device_id_equal(dest, pos->dest))
            break;
    }
#ifdef FWD_THREAD_SAFE
    pthread_mutex_unlock(&tbl->fwdt_lock);
#endif
    return pos;
}

int fwd_table_del(fwd_table_t *tbl, fwd_entry_t *e) {
    assert(tbl);
    assert(e);
#ifdef FWD_THREAD_SAFE
    pthread_mutex_lock(&tbl->fwdt_lock);
#endif
    list_del(&e->ll);
    hash_del(&e->hl);
    tbl->size--;
#ifdef FWD_THREAD_SAFE
    pthread_mutex_unlock(&tbl->fwdt_lock);
#endif
    return 0;
}

static inline void __fwd_table_del_nolock(fwd_table_t *tbl, fwd_entry_t *e) {
    list_del(&e->ll);
    hash_del(&e->hl);
    tbl->size--;
}

int fwd_table_aging(fwd_table_t *tbl) {
    assert(tbl);
    fwd_entry_t **rec = malloc(sizeof(fwd_entry_t) * tbl->size);
    int used = 0;
#ifdef FWD_THREAD_SAFE
    pthread_mutex_lock(&tbl->fwdt_lock);
#endif
    
    fwd_entry_t *pos = NULL;
    list_for_each_entry(pos, &tbl->ll_head, ll) {
        if (pos->status == FWD_EXPIRED) {
            rec[used++] = pos;
        }
        pos->status = FWD_EXPIRED;
    }

    while (used--) {
        __fwd_table_del_nolock(tbl, rec[used]);
        free_fwd_entry(rec[used]);
    }

#ifdef FWD_THREAD_SAFE
    pthread_mutex_unlock(&tbl->fwdt_lock);
#endif
}

//just destroy
void fwd_table_destroy(fwd_table_t *tbl) {
    assert(tbl);
    while (tbl->size--) {
        fwd_entry_t *e = list_entry(tbl->ll_head.next, fwd_entry_t, ll);
        list_del(&e->ll);
        hash_del(&e->hl);
        free_fwd_entry(e);
    }
#ifdef FWD_THREAD_SAFE
    pthread_mutex_destroy(&tbl->fwdt_lock);
#endif
}

//destroy and free allocated memory of tbl
void fwd_table_free(fwd_table_t *tbl) {
    fwd_table_destroy(tbl);
    free(tbl);
}

