#ifndef __NSO_TSD_H__
#define __NSO_TSD_H__

#include <stdint.h>
#include "packet.h"
#include "nso_common.h"
#include "nso_if.h"

enum {
    NSO_TSD_MSG_REG = 0,
    NSO_TSD_MSG_REG_REPLY,
    NSO_TSD_MSG_BS_REG,
    NSO_TSD_MSG_BS_REG_REPLY,
    NSO_TSD_MSG_MAX,
};

typedef struct {
    int vtsd_fd;
}tsd_data_t;

int tsd_init();
int tsd_bs_reg();
int tsd_process_rx(packet_t *pkt, l2addr_t *src, l2addr_t *dst, nso_if_t *iface);

#endif
