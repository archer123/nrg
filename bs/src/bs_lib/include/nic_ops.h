#ifndef __NIC_OPS_INTF_H__
#define __NIC_OPS_INTF_H__

#include "packet.h"
#include "log.h"
#include "nso_common.h"
#include <stdint.h>

struct nic_info_s {
    int mtu;
    //TODO: add more 
};

typedef struct nic_info_s nic_info_t;

typedef void nic_handle_t;

struct nic_ops_s {
    //memory of handle will be allocated by open. It will also be freed by close.
    int (*open)(char *name, nic_handle_t **handle);
    int (*close)(nic_handle_t *handle);
    int (*send)(nic_handle_t *handle, packet_t *p, l2addr_t *dst);
    int (*broadcast)(nic_handle_t *handle, packet_t *p);
    //memory of src and dst will be allocated by receive. User should deal with the memory management after return from receive.
    int (*receive)(nic_handle_t *handle, packet_t *p, l2addr_t **src, l2addr_t **dst);
    int (*get_info)(nic_handle_t *handle, nic_info_t *info);
};

typedef struct nic_ops_s nic_ops_t;

#endif
