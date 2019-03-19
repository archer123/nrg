#ifndef __NSO_COMMON_H__
#define __NSO_COMMON_H__

#include <stdint.h>
#include "hash_crc.h"
#include <assert.h>
#include "log.h"
#include <stdlib.h>
#include <string.h>

typedef uint64_t device_id_t;

#define DEV_ID_WIDTH sizeof(device_id_t)

static device_id_t* alloc_device_id(uint8_t *val) {
    device_id_t *id = malloc(sizeof(device_id_t));
    if (!id) {
        LOG_DEBUG("device id alloc failed\n");
        return NULL;
    }
    if (val) {
        memcpy(id, val, DEV_ID_WIDTH);
    }
    return id;
}


static void free_device_id(device_id_t *dev_id) {
    if (dev_id) free(dev_id);
}

static uint32_t hash_device_id(device_id_t *dev_id) {
    assert(dev_id);
    return hash_crc((const void*)dev_id, sizeof(device_id_t), 0);
}

static void assign_device_id(device_id_t *dev_id, uint8_t *val) {
    assert(dev_id);
    assert(val);
    memcpy(dev_id, val, DEV_ID_WIDTH);
}

//broadcast device_id
static inline device_id_t* alloc_bc_device_id() {
    device_id_t *id = malloc(sizeof(device_id_t));
    if (!id) {
        LOG_DEBUG("alloc broadcast device id failed!\n");
        return NULL;
    }
    *id = 0xffffffffffffffffULL;
    return id;
}

static int device_id_equal(device_id_t *id1, device_id_t *id2) {
    return *id1 == *id2;
}

static int is_broadcast_dev_id(device_id_t *dev_id) {
    device_id_t *id = alloc_bc_device_id();
    int ret = device_id_equal(dev_id, id);
    free_device_id(id);
    return ret;
}

struct l2addr_s {
    int size;
    uint8_t addr[0];
}__attribute__((packed));

typedef struct l2addr_s l2addr_t;

static l2addr_t* alloc_l2addr(int size, uint8_t *val) {
    l2addr_t *addr = malloc(sizeof(l2addr_t) + size);
    if (!addr) {
        LOG_DEBUG("l2addr allocation failed\n");
        return NULL;
    }
    addr->size = size;
    if (val) {
        memcpy(addr->addr, val, addr->size);
    }
    return addr;
}

static void free_l2addr(l2addr_t *addr) {
    if (addr) free(addr);
}

static uint32_t hash_l2addr(l2addr_t *addr) {
    assert(addr);
    return hash_crc(addr->addr, addr->size, 0);
}

static void assign_l2addr(l2addr_t *addr, uint8_t *val) {
    assert(addr);
    assert(val);
    memcpy(addr->addr, val, addr->size);
}

static int l2addr_equal(l2addr_t *a1, l2addr_t *a2) {
    return ((a1->size == a2->size) &&
            memcmp(a1->addr, a2->addr, a1->size) == 0);
}

#endif
