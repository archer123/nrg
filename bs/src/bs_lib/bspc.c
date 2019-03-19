#include "bspc.h"

bspc_table_t* bspc_table_create() {
    bspc_table_t *tbl = malloc(sizeof(bspc_table_t));
    if (!tbl) {
        LOG_DEBUG("fwd table create failed!\n");
        return NULL;
    }
    bspc_table_init(tbl);
    return tbl;
}

void bspc_table_init(bspc_table_t *tbl) {
    assert(tbl);
    tbl->size = 0;
    INIT_LIST_HEAD(&tbl->ll_head);
    hash_init(tbl->ht_proto);
}

int bspc_table_add(bspc_table_t *tbl, bspc_entry_t *e) {
    assert(tbl);
    assert(e);
    list_add(&e->ll, &tbl->ll_head);
    hash_add(tbl->ht_proto, &e->hl, e->proto);
    tbl->size++;
    return 0;
}

bspc_entry_t* bspc_table_lookup(bspc_table_t *tbl, uint16_t proto) {
    assert(tbl);
    bspc_entry_t *pos = NULL;
    hash_for_each_possible(tbl->ht_proto, pos, hl, proto) {
        if (pos->proto == proto)
            break;
    }
    return pos;
}

int bspc_table_del(bspc_table_t *tbl, bspc_entry_t *e) {
    assert(tbl);
    assert(e);
    list_del(&e->ll);
    hash_del(&e->hl);
    tbl->size--;
    return 0;
}

//just destroy
void bspc_table_destroy(bspc_table_t *tbl) {
    assert(tbl);
    while (tbl->size--) {
        bspc_entry_t *e = list_entry(tbl->ll_head.next, bspc_entry_t, ll);
        list_del(&e->ll);
        hash_del(&e->hl);
        free_bspc_entry(e);
    }
}

//destroy and free allocated memory of tbl
void bspc_table_free(bspc_table_t *tbl) {
    bspc_table_destroy(tbl);
    free(tbl);
}

