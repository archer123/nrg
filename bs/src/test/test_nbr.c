#include "neighbor.h"
#include <stdio.h>
#include <stdint.h>
#include "nso_if.h"

void test() {
    nbr_table_t *tbl = nbr_table_create();
    assert(tbl);

    uint64_t ID = 0x66667874U;

    device_id_t *dest = alloc_device_id((uint8_t*)&ID);
    nso_if_t nif = {
        .if_name = "test_nbr",
        .if_index = 0,
    };
    metric_t *m = alloc_metric(1024);
    nbr_entry_t *e1 = alloc_nbr_entry(dest, m, &nif);

    ID = 0x77776639U;
    dest = alloc_device_id((uint8_t*)&ID);

    metric_t *m2 = alloc_metric(2048);
    nbr_entry_t *e2 = alloc_nbr_entry(dest, m, &nif);

    nbr_table_add(tbl, e1);
    nbr_table_add(tbl, e2);

    nbr_entry_t *res = NULL;

    ID = 0x77776639U;
    device_id_t *key = alloc_device_id((uint8_t*)&ID);

    res = nbr_table_lookup(tbl, key);
    assert(res == e2);
    
    printf("dest: %llx, metric: %d, if: %s, status: %s\n",
            *(uint64_t*)res->dev_id, res->metric->w,
            res->iface->if_name,
            res->status == NBR_ACTIVE ? "active": "expired");

    ID = 0x66667874U;
    assign_device_id(key, (uint8_t*)&ID);

    res = nbr_table_lookup(tbl, key);
    assert(res == e1);
    
    printf("dest: %llx, metric: %d, if: %s, status: %s\n",
            *(uint64_t*)res->dev_id, res->metric->w,
            res->iface->if_name,
            res->status == NBR_ACTIVE ? "active": "expired");

    nbr_table_aging(tbl);

    assert(e1->status == NBR_EXPIRED);
    assert(e2->status == NBR_EXPIRED);

    nbr_table_del(tbl, e1);
    free_nbr_entry(e1);

    res = nbr_table_lookup(tbl, key);
    assert(res == NULL);

    nbr_table_free(tbl);
    
    free_device_id(key);
}

int main(int argc, char **argv) {
    test();
    return 0;
}
