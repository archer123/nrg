#ifndef __PACKET_QUEUE_H__
#define __PACKET_QUEUE_H__

#include "list.h"
#include "packet.h"
#include <pthread.h>

typedef struct {
    packet_t *pkt;
    struct list_head ll;
}pq_entry_t;

typedef struct {
    struct list_head pq_head;
}pq_t;


void pq_put_packet(pq_t *pq, packet_t *pkt);

packet_t* pq_get_packet(pq_t *pq);

void pq_init(pq_t *pq);
int pq_empty(pq_t *pq);
void pq_destroy(pq_t *pq);

#endif
