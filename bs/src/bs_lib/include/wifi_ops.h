#ifndef __WIFI_OPS_H__
#define __WIFI_OPS_H__

#include "packet.h"
#include "nic_ops.h"

#define ETH_TYPE_NSO 0xF748

struct wifi_handle_s {
    int sockfd;
    int if_index;
    char if_mac[ETH_ALEN];
    int if_mtu;
};

typedef struct wifi_handle_s wifi_handle_t;

int wifi_open(char *name, nic_handle_t **handle);
int wifi_close(nic_handle_t *handle);
int wifi_send(nic_handle_t *handle, packet_t *pkt, l2addr_t *dst);
int wifi_broadcast(nic_handle_t *handle, packet_t *pkt);
int wifi_receive(nic_handle_t *handle, packet_t *pkt, l2addr_t **src, l2addr_t **dst);
int wifi_get_info(nic_handle_t *handle, nic_info_t *info);

#endif
