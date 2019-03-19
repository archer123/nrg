#include "neighbor.h"
#include <assert.h>

nbr_table_t* nbr_table_create() {
    nbr_table_t *nbrt = malloc(sizeof(nbr_table_t));
    if (!nbrt) {
        LOG_DEBUG("nbr table create failed!\n");
        return NULL;
    }
    nbr_table_init(nbrt);
    return nbrt;
}

void nbr_table_init(nbr_table_t *tbl) {
    assert(tbl);
    tbl->size = 0;
    INIT_LIST_HEAD(&tbl->ll_head);
    hash_init(tbl->ht_dev_id);
    pthread_mutex_init(&tbl->nbrt_lock, NULL);
}

//shoude be surounded by lock and unlock
int nbr_table_size(nbr_table_t *tbl) {
    return tbl->size;
}

//add new entry
int nbr_table_add(nbr_table_t *tbl, nbr_entry_t *e) {
    assert(tbl);
    assert(e);

    pthread_mutex_lock(&tbl->nbrt_lock);

    list_add(&e->ll, &tbl->ll_head);
    hash_add(tbl->ht_dev_id, &e->hl_devid, hash_device_id(e->dev_id));
    tbl->size++;

    pthread_mutex_unlock(&tbl->nbrt_lock);

    return 0;
}
//lookup entry from dev_id
nbr_entry_t* nbr_table_lookup(nbr_table_t *tbl, device_id_t *dev_id) {
    assert(tbl);
    assert(dev_id);

    pthread_mutex_lock(&tbl->nbrt_lock);

    nbr_entry_t *pos = NULL;

    hash_for_each_possible(tbl->ht_dev_id, pos, hl_devid, hash_device_id(dev_id)) {
        if (device_id_equal(dev_id, pos->dev_id))
            break;
    }

    pthread_mutex_unlock(&tbl->nbrt_lock);

    return pos;
}


//del entry
int nbr_table_del(nbr_table_t *tbl, nbr_entry_t *e) {
    assert(tbl);
    assert(e);
    pthread_mutex_lock(&tbl->nbrt_lock);
    
    list_del(&e->ll);
    hash_del(&e->hl_devid);
    tbl->size--;

    pthread_mutex_unlock(&tbl->nbrt_lock);
    return 0;
}

static inline void __nbr_table_del_nolock(nbr_table_t *tbl, nbr_entry_t *e) {
    list_del(&e->ll);
    hash_del(&e->hl_devid);
    tbl->size--;
}

//aging
int nbr_table_aging(nbr_table_t *tbl) {
    assert(tbl);
    //record entries which should be deleted
    nbr_entry_t **rec = malloc(sizeof(nbr_entry_t*) * tbl->size);
    int used = 0;

    pthread_mutex_lock(&tbl->nbrt_lock);

    nbr_entry_t *pos = NULL;
    list_for_each_entry(pos, &tbl->ll_head, ll) {
        if (pos->status == NBR_EXPIRED) {
            rec[used++] = pos;
        }
        pos->status = NBR_EXPIRED;
    }

    while(used--) {
        __nbr_table_del_nolock(tbl, rec[used]);
        free_nbr_entry(rec[used]);
    }

    pthread_mutex_unlock(&tbl->nbrt_lock);

    return 0;
}

void nbr_table_destroy(nbr_table_t *tbl) {
    assert(tbl);
    while (tbl->size--) {
        nbr_entry_t *e = list_entry(tbl->ll_head.next, nbr_entry_t, ll);
        list_del(&e->ll);
        hash_del(&e->hl_devid);
        free_nbr_entry(e);
    }
    pthread_mutex_destroy(&tbl->nbrt_lock);
}

void nbr_table_free(nbr_table_t *tbl) {
    nbr_table_destroy(tbl);
    free(tbl);
}

void nbr_table_lock(nbr_table_t *tbl) {
    pthread_mutex_lock(&tbl->nbrt_lock);
}

void nbr_table_unlock(nbr_table_t *tbl) {
    pthread_mutex_unlock(&tbl->nbrt_lock);
}

nbr_entry_t* nbr_table_lookup_unsafe(nbr_table_t *tbl, device_id_t *dev_id) {
    assert(tbl);
    assert(dev_id);
    nbr_entry_t *pos = NULL;

    hash_for_each_possible(tbl->ht_dev_id, pos, hl_devid, hash_device_id(dev_id)) {
        if (device_id_equal(dev_id, pos->dev_id))
            break;
    }
    return pos;
}

