#ifndef __NSO_SON_H__
#define __NSO_SON_H__

#include "packet.h"
#include "nso_if.h"
#include "nso_common.h"

#include <stdint.h>

enum {
    NSO_SON_MSG_TOPO = 0,
    NSO_SON_MSG_RT,
    NSO_SON_MSG_NBR,
    NSO_SON_MSG_MAX,
};

//initialization
int son_init();
//report topology information to vSON
int son_topo_report();
//broadcast to neighbors to maintain the ARP table of neighbors
int son_nbr_advertise();

//pkt->data should point to the start of payload of nso packet
int son_process_rx(packet_t *pkt, l2addr_t *src, l2addr_t *dst, nso_if_t *iface);

#define RT_ACTION_ADD 0
#define RT_ACTION_DEL 1

typedef struct __attribute__((packed)){
    uint8_t action;
    uint64_t dst_id;
    uint64_t nh_id;
    uint8_t if_index;
}rt_rule_t;

#endif
