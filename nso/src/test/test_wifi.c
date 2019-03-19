#include "wifi_ops.h"
#include "packet.h"
#include <stdio.h>
#include <net/ethernet.h>
#include <arpa/inet.h>

static char dst_mac[6] = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

static packet_t* prepare_packet() {
    packet_t *pkt = alloc_packet(1024);
    pkt->data[pkt->byte_len++] = 0xde;
    pkt->data[pkt->byte_len++] = 0xad;
    pkt->data[pkt->byte_len++] = 0xbe;
    pkt->data[pkt->byte_len++] = 0xef;
    return pkt;
}

int main(int argc, char **argv) {

    if (argc != 2) {
        fprintf(stderr, "argv[1] = interface name\n");
        exit(-1);
    }

    char *ifname = argv[1];
    nic_handle_t *handle;

    if (wifi_open(ifname, &handle) < 0) {
        fprintf(stderr, "wifi open failed\n");
        exit(-1);
    }

    while (1) {
        packet_t *pkt = prepare_packet();
        l2addr_t *addr = alloc_l2addr(ETH_ALEN, NULL);
        memcpy(addr->addr, dst_mac, ETH_ALEN);
        wifi_send(handle, pkt, addr);
        free_packet(pkt);
        free_l2addr(addr);
        pkt = alloc_packet(1024);
        l2addr_t *src, *dst;
        wifi_receive(handle, pkt, &src, &dst);
        printf("received pkt %d bytes\n", pkt->byte_len);
#define smac(x) src->addr[x]
#define dmac(x) dst->addr[x]
        printf("from %02X:%02X:%02X:%02X:%02X:%02X to %02X:%02X:%02X:%02X:%02X:%02X\n",
                smac(0), smac(1), smac(2), smac(3), smac(4), smac(5),
                dmac(0), dmac(1), dmac(2), dmac(3), dmac(4), dmac(5));
#undef smac
#undef dmac
        free_l2addr(src);
        free_l2addr(dst);
        free_packet(pkt);
    }

    return 0;
}
