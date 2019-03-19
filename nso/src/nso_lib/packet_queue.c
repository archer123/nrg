#include "packet_queue.h"

void pq_put_packet(pq_t *pq, packet_t *pkt) {
    pq_entry_t *pq_e = (pq_entry_t*)malloc(sizeof(pq_entry_t));
    pq_e->pkt = pkt;
    list_add_tail(&pq_e->ll, &pq->pq_head);
}

packet_t* pq_get_packet(pq_t *pq) {
    pq_entry_t *pq_e = NULL;
    packet_t *pkt = NULL;
    if (!list_empty(&pq->pq_head)) {
        pq_e = list_first_entry(&pq->pq_head, pq_entry_t, ll);
        list_del(&pq_e->ll);
        pkt = pq_e->pkt;
        free(pq_e);
    }
    return pkt;
}

void pq_init(pq_t *pq) {
    INIT_LIST_HEAD(&pq->pq_head);
}

void pq_destroy(pq_t *pq) {
    //remove all elems
    while (!list_empty(&pq->pq_head)) {
        pq_entry_t *pq_e = list_first_entry(&pq->pq_head, pq_entry_t, ll);
        list_del(&pq_e->ll);
        free_packet(pq_e->pkt);
        free(pq_e);
    }
}

int pq_empty(pq_t *pq) {
    return list_empty(&pq->pq_head);
}

