#include "arp.h"
#include <stdio.h>
#include "nso_common.h"
#include <linux/if_ether.h>

//TODO: unittest of arp module

void test() {
    arp_table_t *tbl = arp_table_create();
    l2addr_t *addr = alloc_l2addr(ETH_ALEN, NULL);
    uint64_t ID = 0x66667487ULL;
    device_id_t *dev_id = alloc_device_id((uint8_t*)&ID);
    char mac[6] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06};
    assign_l2addr(addr, mac);

    arp_entry_t *e = alloc_arp_entry(dev_id, addr);
    printf("id:%llX, addr:%06llX\n", *(e->dev_id), *((uint64_t*)(e->l2addr->addr)));

    arp_table_add(tbl, e);
    arp_entry_t *res = arp_table_lookup_from_dev_id(tbl, dev_id);
    assert(res);
    printf("id:%llX, addr:%06llX\n", *(res->dev_id), *((uint64_t*)(res->l2addr->addr)));
    
    res = arp_table_lookup_from_l2addr(tbl, addr);
    assert(res);
    printf("id:%llX, addr:%06llX\n", *(res->dev_id), *((uint64_t*)(res->l2addr->addr)));

    arp_table_aging(tbl);
    assert(e->status == ARP_EXPIRED);

    arp_table_del(tbl, res);
    assert(res == e);

    res = arp_table_lookup_from_dev_id(tbl, dev_id);
    assert(res == NULL);
    res = arp_table_lookup_from_l2addr(tbl, addr);
    assert(res == NULL);

    arp_table_free(tbl);
    free_arp_entry(e);
}


int main(int argc, char **argv) {

    test();

    return 0;
}
