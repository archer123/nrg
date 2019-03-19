#ifndef __NSO_IF_H__
#define __NSO_IF_H__

#include "nic_ops.h"
#include "wifi_ops.h"
#include "log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_NSO_IFNAME_SZ 128

#define IFACE_TYPE_MIN 0
#define IFACE_TYPE_WIFI IFACE_TYPE_MIN
//#define IFACE_TYPE_802154 1
#define IFACE_TYPE_MAX 1

typedef struct nso_if_s {
    nic_handle_t *handle;
    nic_ops_t *ops;
    char if_name[MAX_NSO_IFNAME_SZ];
    int if_index;
    int if_type;
}nso_if_t;

static nic_ops_t nic_ops[] = {
    //wifi ops
    [IFACE_TYPE_WIFI] = {
        .open = wifi_open,
        .close = wifi_close,
        .send = wifi_send,
        .broadcast = wifi_broadcast,
        .receive = wifi_receive,
        .get_info = wifi_get_info,
    },
};

static nso_if_t* alloc_nso_if(char *if_name, int index, int type) {
    nso_if_t *iface = malloc(sizeof(nso_if_t));
    if (!iface) {
        LOG_DEBUG("no enough memory!\n");
        return NULL;
    }
    strcpy(iface->if_name, if_name);
    iface->if_index = index;
    iface->if_type = type;
    iface->ops = &nic_ops[type];
    return iface;
}

static void free_nso_if(nso_if_t *iface) {
    free(iface);
}


/*abstract interface operation*/
static int nso_if_open(nso_if_t *iface) {
    int ret = iface->ops->open(iface->if_name, &iface->handle);
    return ret;
}

static int nso_if_close(nso_if_t *iface) {
    int ret = iface->ops->close(iface->handle);
    return ret;
}

static int nso_if_send(nso_if_t *iface, packet_t *p, l2addr_t *dst) {
    return iface->ops->send(iface->handle, p, dst);
}

static int nso_if_broadcast(nso_if_t *iface, packet_t *p) {
    return iface->ops->broadcast(iface->handle, p);
}

static int nso_if_receive(nso_if_t *iface, packet_t *p, l2addr_t **src, l2addr_t **dst) {
    return iface->ops->receive(iface->handle, p, src, dst);
}

static int nso_if_get_info(nso_if_t *iface, nic_info_t *info) {
    return iface->ops->get_info(iface->handle, info);
}

#endif
