#include "fwding.h"
#include <stdio.h>
#include <stdint.h>
#include "nso_if.h"

void test() {
    fwd_table_t *tbl = fwd_table_create();
    assert(tbl);

    uint64_t ID = 0x66667874U;

    device_id_t *dest = alloc_device_id((uint8_t*)&ID);
    nso_if_t nif = {
        .if_name = "test_fwd",
        .if_index = 0,
    };
    fwd_entry_t *e1 = alloc_fwd_entry(dest, NULL, &nif);

    ID = 0x77776639U;
    dest = alloc_device_id((uint8_t*)&ID);
    ID = 0x88889898U;
    device_id_t *nxt = alloc_device_id((uint8_t*)&ID);
    fwd_entry_t *e2 = alloc_fwd_entry(dest, nxt, &nif);

    fwd_table_add(tbl, e1);
    fwd_table_add(tbl, e2);

    fwd_entry_t *res = NULL;

    ID = 0x77776639U;
    device_id_t *key = alloc_device_id((uint8_t*)&ID);

    res = fwd_table_lookup(tbl, key);
    assert(res == e2);
    
    printf("dest: %llx, nxt: %llx, if: %s, status: %s\n",
            *(uint64_t*)res->dest, *(uint64_t*)res->nxthop,
            res->interface->if_name,
            res->status == FWD_ACTIVE ? "active": "expired");

    ID = 0x66667874U;
    assign_device_id(key, (uint8_t*)&ID);

    res = fwd_table_lookup(tbl, key);
    assert(res == e1);
    
    printf("dest: %llx, nxt: %llx, if: %s, status: %s\n",
            *(uint64_t*)res->dest, *(uint64_t*)res->nxthop,
            res->interface->if_name,
            res->status == FWD_ACTIVE ? "active": "expired");

    fwd_table_aging(tbl);

    assert(e1->status == FWD_EXPIRED);
    assert(e2->status == FWD_EXPIRED);

    fwd_table_del(tbl, e1);
    free_fwd_entry(e1);

    res = fwd_table_lookup(tbl, key);
    assert(res == NULL);

    fwd_table_free(tbl);
    
    free_device_id(key);
}

int main(int argc, char **argv) {
    test();
    return 0;
}
