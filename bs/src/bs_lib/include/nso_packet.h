#ifndef __NSO_PACKET_H__
#define __NSO_PACKET_H__

#include <stdint.h>
#include "nso_common.h"

#define NSO_VERSION 0

#define NSO_PROTO_IP 0x0800
#define NSO_PROTO_IPv6 0x86dd
#define NSO_PROTO_CP_VTSD 0xF000
#define NSO_PROTO_CP_VSON 0xF001
#define NSO_PROTO_IS_CP(proto) ((proto & 0xFFFE) == 0xF000)

struct nsohdr {
    uint8_t src_devid[DEV_ID_WIDTH];
    uint8_t dst_devid[DEV_ID_WIDTH];
    uint16_t proto;
    //length including header size
    union{
        uint16_t length: 14, //low 14 bits
                 ver: 2; //high 2 bits
        uint16_t len_ver;
    };
}__attribute__((packed));

#endif
