#include "arp.h"
#include <assert.h>

arp_table_t* arp_table_create() {
    arp_table_t *arpt = malloc(sizeof(arp_table_t));
    if (!arpt) {
        LOG_DEBUG("arp table create failed!\n");
        return NULL;
    }
    arp_table_init(arpt);
    return arpt;
}

void arp_table_init(arp_table_t *tbl) {
    assert(tbl);
    tbl->size = 0;
    INIT_LIST_HEAD(&tbl->ll_head);
    hash_init(tbl->ht_dev_id);
    hash_init(tbl->ht_l2addr);
    pthread_mutex_init(&tbl->arpt_lock, NULL);
}

//add new entry
int arp_table_add(arp_table_t *tbl, arp_entry_t *e) {
    assert(tbl);
    assert(e);

    pthread_mutex_lock(&tbl->arpt_lock);

    list_add(&e->ll, &tbl->ll_head);
    hash_add(tbl->ht_dev_id, &e->hl_devid, hash_device_id(e->dev_id));
    hash_add(tbl->ht_l2addr, &e->hl_l2addr, hash_l2addr(e->l2addr));
    tbl->size++;

    pthread_mutex_unlock(&tbl->arpt_lock);

    return 0;
}
//lookup entry from dev_id
arp_entry_t* arp_table_lookup_from_dev_id(arp_table_t *tbl, device_id_t *dev_id) {
    assert(tbl);
    assert(dev_id);

    pthread_mutex_lock(&tbl->arpt_lock);

    arp_entry_t *pos = NULL;

    hash_for_each_possible(tbl->ht_dev_id, pos, hl_devid, hash_device_id(dev_id)) {
        if (device_id_equal(dev_id, pos->dev_id))
            break;
    }

    pthread_mutex_unlock(&tbl->arpt_lock);

    return pos;
}

//lookup entry from l2addr
arp_entry_t* arp_table_lookup_from_l2addr(arp_table_t *tbl, l2addr_t *addr) {
    assert(tbl);
    assert(addr);

    pthread_mutex_lock(&tbl->arpt_lock);

    arp_entry_t *pos = NULL;

    hash_for_each_possible(tbl->ht_l2addr, pos, hl_l2addr, hash_l2addr(addr)) {
        if (l2addr_equal(addr, pos->l2addr))
            break;
    }

    pthread_mutex_unlock(&tbl->arpt_lock);

    return pos;
}

//del entry
int arp_table_del(arp_table_t *tbl, arp_entry_t *e) {
    assert(tbl);
    assert(e);
    pthread_mutex_lock(&tbl->arpt_lock);
    
    list_del(&e->ll);
    hash_del(&e->hl_devid);
    hash_del(&e->hl_l2addr);
    tbl->size--;

    pthread_mutex_unlock(&tbl->arpt_lock);
    return 0;
}

static inline void __arp_table_del_nolock(arp_table_t *tbl, arp_entry_t *e) {
    list_del(&e->ll);
    hash_del(&e->hl_devid);
    hash_del(&e->hl_l2addr);
    tbl->size--;
}

//aging
int arp_table_aging(arp_table_t *tbl) {
    assert(tbl);
    //record entries which should be deleted
    arp_entry_t **rec = malloc(sizeof(arp_entry_t*) * tbl->size);
    int used = 0;

    pthread_mutex_lock(&tbl->arpt_lock);

    arp_entry_t *pos = NULL;
    list_for_each_entry(pos, &tbl->ll_head, ll) {
        if (pos->status == ARP_EXPIRED) {
            rec[used++] = pos;
        }
        pos->status = ARP_EXPIRED;
    }

    while(used--) {
        __arp_table_del_nolock(tbl, rec[used]);
        free_arp_entry(rec[used]);
    }

    pthread_mutex_unlock(&tbl->arpt_lock);

    return 0;
}

void arp_table_destroy(arp_table_t *tbl) {
    assert(tbl);
    while (tbl->size--) {
        arp_entry_t *e = list_entry(tbl->ll_head.next, arp_entry_t, ll);
        list_del(&e->ll);
        hash_del(&e->hl_devid);
        hash_del(&e->hl_l2addr);
        free_arp_entry(e);
    }
    pthread_mutex_destroy(&tbl->arpt_lock);
}

void arp_table_free(arp_table_t *tbl) {
    arp_table_destroy(tbl);
    free(tbl);
}
