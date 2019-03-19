#ifndef __PACKET_H__
#define __PACKET_H__

#include <stdlib.h>
#include <string.h>
#include <linux/if_ether.h>
#include "nso_packet.h"

#define HEADROOM 64
#define TAILROOM 64

#define parse_hdr(pkt, type) \
    {type *hdr = (type*)pkt->data;\
     pkt->data += sizeof(type); \
     pkt->byte_len -= sizeof(type);\
     hdr;}

struct packet_s {
    //size means the total size of buf
    int size;
    //byte_len is the current used size of buf
    int byte_len;
    char *data;
    char *tail;
    char buf[0];
}__attribute__((packed));

typedef struct packet_s packet_t;

static packet_t* alloc_packet(int size) {
    packet_t *p = malloc(sizeof(packet_t) + size + HEADROOM + TAILROOM);
    if (!p) {
        return NULL;
    }
    memset(p, 0, sizeof(packet_t) + size);
    p->byte_len = 0;
    p->size = size;
    p->data = p->buf + HEADROOM;
    p->tail = p->data;
    return p;
}

static void free_packet(packet_t *p) {
    if(p) free(p);
}

static int packet_append_data(packet_t *p, uint8_t *data, int size) {
    assert(p);
    assert(data);
    if ((p->size - p->byte_len) < size) {
        LOG_DEBUG("packet_append_data fail, don't have enough space\n");
        return -1;
    }
    memcpy(p->data, data, size);
    p->byte_len += size;
    p->tail = p->data + p->byte_len;
    return 0;
}

static void packet_inc_len(packet_t *p, int size) {
    p->byte_len += size;
    p->tail += size;
}

static void packet_pop_head(packet_t *p, int size) {
    p->byte_len -= size;
    p->data += size;
}

static void packet_reset(packet_t *p) {
    p->byte_len = 0;
    p->data = p->buf + HEADROOM;
    p->tail = p->data;
}

static packet_t* packet_clone(packet_t *pkt) {
    packet_t *new_pkt;
    new_pkt = alloc_packet(pkt->size);
    new_pkt->byte_len = pkt->byte_len;
    new_pkt->data = pkt->data;
    new_pkt->tail = pkt->tail;
    memcpy(new_pkt->data, pkt->data, pkt->byte_len);
    return new_pkt;
}

#endif
