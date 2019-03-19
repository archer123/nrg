#include "nso_son.h"
#include "neighbor.h"
#include "nso.h"
#include "fwding.h"
#include "arp.h"
#include "nso.h"
#include <arpa/inet.h>
#include "bs_util.h"

extern nso_layer_t nso_layer;
static son_data_t son;

int son_init() {
    nso_layer_t *nsol = &nso_layer;
    bspc_entry_t *pos = bspc_table_lookup(nsol->bspc_tbl, NSO_PROTO_CP_VSON);
    son.vson_fd = pos->sockfd;
    return 0;
}

/*
 * msg format:
 * [nso hdr]
 * [type 1B]
 * [topo data]
 *
 * topo data format:
 * <battery value 1B>
 * <number of interfaces 1B>
 * <iface index 1B> <iface quality 1B>
 * ...
 * <number of neighbors 2B>
 * <neighbor device id 8B> <neighbor link quality 1B> <iface index 1B>
 * ...
 * */

static packet_t* __get_topo_report_pkt(nso_layer_t *nsol) {
    packet_t *pkt =  alloc_packet(nsol->mtu);
    //fill in data
    struct nsohdr *nso_hdr = (struct nsohdr*)pkt->data;

    memcpy(nso_hdr->src_devid, (uint8_t*)nsol->dev_id, DEV_ID_WIDTH);

    //to gateway
    memcpy(nso_hdr->dst_devid, (uint8_t*)nsol->gw_id, DEV_ID_WIDTH);

    nso_hdr->proto = htons(NSO_PROTO_CP_VSON);
    nso_hdr->ver = 0;

    uint8_t *type = pkt->data + sizeof(struct nsohdr);
    uint8_t *data = type + 1;

    *type = NSO_SON_MSG_TOPO;
    
    //preprare the data 
    *data = nsol->battery;
    data += 1;
    *data = (uint8_t)nsol->ifaces_nb;
    data += 1;
    int ifid;
    for (ifid = 0; ifid < nsol->ifaces_nb; ifid++) {
        nso_if_t *iface = nsol->ifaces[ifid];
        *data = (uint8_t)ifid;
        //TODO: get interface quality from device, instead of using static value
        *(data + 1) = 0;
        data += 2;
    }

    nbr_table_lock(nsol->nbrt);

    *(uint16_t*)data = htons(nbr_table_size(nsol->nbrt));
    data += 2;

    nbr_entry_t *entry;
    nbr_table_iterate(entry, nsol->nbrt) {
        memcpy(data, (uint8_t*)entry->dev_id, DEV_ID_WIDTH);
        *(data + DEV_ID_WIDTH) = (uint8_t)entry->metric->w;
        *(data + DEV_ID_WIDTH + 1) = (uint8_t)entry->iface->if_index;
        data += DEV_ID_WIDTH + 2;
    }

    nbr_table_unlock(nsol->nbrt);

    nso_hdr->length = data - type + sizeof(struct nsohdr);

    //maintain pointers
    packet_inc_len(pkt, nso_hdr->length);
    nso_hdr->len_ver = htons(nso_hdr->len_ver);
    return pkt;
}

int son_topo_report() {
    nso_layer_t *nsol = &nso_layer;
    packet_t *pkt = __get_topo_report_pkt(nsol);
    if (!pkt) {
        return -1;
    }
    send_pkt_to_vnf(son.vson_fd, pkt);
    free_packet(pkt);
    return 0;
}

static packet_t* __get_nbr_adv_pkt(nso_layer_t *nsol) {
    packet_t *pkt =  alloc_packet(nsol->mtu);
    //fill in data
    struct nsohdr *nso_hdr = (struct nsohdr*)pkt->data;

    memcpy(nso_hdr->src_devid, (uint8_t*)nsol->dev_id, DEV_ID_WIDTH);

    device_id_t *bc_id = alloc_bc_device_id();
    memcpy(nso_hdr->dst_devid, (uint8_t*)bc_id, DEV_ID_WIDTH);
    free_device_id(bc_id);

    nso_hdr->proto = htons(NSO_PROTO_CP_VSON);
    nso_hdr->ver = 0;

    uint8_t *type = pkt->data + sizeof(struct nsohdr);

    *type = NSO_SON_MSG_NBR;

    nso_hdr->length = sizeof(struct nsohdr) + sizeof(*type);
    
    //maintain pointers
    packet_inc_len(pkt, nso_hdr->length);
    nso_hdr->len_ver = htons(nso_hdr->len_ver);
    return pkt;
}

int son_nbr_advertise() {
    nso_layer_t *nsol = &nso_layer;
    packet_t *pkt = __get_nbr_adv_pkt(nsol);
    if (!pkt) {
        return -1;
    }
    //advertising to all interfaces
    int i;
    for (i = 0; i < nsol->ifaces_nb; i++) {
        nso_if_t *iface = nsol->ifaces[i];
        if (nso_if_broadcast(iface, pkt) < 0) {
            LOG_DEBUG("advertise neighborship failed in iface %s\n", iface->if_name);
        }
    }
    free_packet(pkt);
    return 0;
}


/*
 * message format:
 * <nsohdr>
 * <1B type>
 * <1B action> <8B dst_devid> <8B nxthop_devid> <1B if_index>
 * .......
 *
 * action:
 * 0 = add
 * 1 = del
 * */
static void __route_update(nso_layer_t *nsol, packet_t *pkt, nso_if_t *iface) {
    struct nsohdr *hdr = (struct nsohdr*)pkt->data;
    device_id_t *pkt_dst_id = alloc_device_id((uint8_t*)hdr->dst_devid);

    if (device_id_equal(pkt_dst_id, nsol->dev_id)) {
        //is for me
        pthread_mutex_lock(&nsol->state_lock);
        int ret = nsol->dev_state == NRG5_REG || nsol->dev_state == NRG5_CONNECTED;
        pthread_mutex_unlock(&nsol->state_lock);
        if (!ret) {
            LOG_DEBUG("drop received route update pkt\n");
            goto exit;
        }
        //update routes
        uint8_t *rt_data = pkt->data + sizeof(*hdr) + 1;
        int size = pkt->byte_len - sizeof(*hdr) - 1;
        rt_rule_t *rule;
        while (size > 0) {
            rule = (rt_rule_t*)rt_data;
            device_id_t *dst_id = alloc_device_id((uint8_t*)&rule->dst_id);
            device_id_t *nh_id = alloc_device_id((uint8_t*)&rule->nh_id);
            nso_if_t *iface = nsol->ifaces[rule->if_index];
            fwd_entry_t *fwd_e = fwd_table_lookup(nsol->son_fwdt, dst_id);
            switch(rule->action) {
                case RT_ACTION_ADD:
                    if (fwd_e) {
                        fwd_table_del(nsol->son_fwdt, fwd_e);
                        free_fwd_entry(fwd_e);
                    }
                    fwd_e = alloc_fwd_entry(dst_id, nh_id, iface);
                    fwd_table_add(nsol->son_fwdt, fwd_e);
                    break;
                case RT_ACTION_DEL:
                    if (fwd_e) {
                        fwd_table_del(nsol->son_fwdt, fwd_e);
                        free_fwd_entry(fwd_e);
                    }
                    free_device_id(dst_id);
                    free_device_id(nh_id);
                    break;
                default:
                    free_device_id(dst_id);
                    free_device_id(nh_id);
                    LOG_DEBUG("unknown route rule action!\n");
            }
            size -= sizeof(rt_rule_t);
            rt_data += sizeof(rt_rule_t);
        }
        assert(size == 0);
    } else {
        pthread_mutex_lock(&nsol->state_lock);
        int ret = nsol->dev_state == NRG5_CONNECTED;
        pthread_mutex_unlock(&nsol->state_lock);
        if (!ret) {
            LOG_DEBUG("drop receieved route update!\n");
            goto exit;
        }
        nso_layer_fwd(pkt);
    }
exit:
    free_device_id(pkt_dst_id);
}

static void __neighbor_maintain(nso_layer_t *nsol, packet_t *pkt, l2addr_t *src, l2addr_t *dst, nso_if_t *iface) {
    struct nsohdr *hdr = (struct nsohdr*)pkt->data;
    device_id_t *dev_id = alloc_device_id(hdr->src_devid);
    //maintain arp table
    arp_table_lock(nsol->arpt);
    arp_entry_t *arp_e = arp_table_lookup_from_dev_id_unsafe(nsol->arpt, dev_id);
    if (arp_e) {
        arp_e->status = ARP_ACTIVE;
        arp_table_unlock(nsol->arpt);
    } else {
        arp_table_unlock(nsol->arpt);
        device_id_t *dev_id_copy = alloc_device_id(hdr->src_devid);
        l2addr_t *mac = alloc_l2addr(src->size, src->addr);
        arp_e = alloc_arp_entry(dev_id_copy, mac);
        arp_table_add(nsol->arpt, arp_e);
    }
    //maintain neighbor table
    nbr_table_lock(nsol->nbrt);
    nbr_entry_t *nbr_e = nbr_table_lookup_unsafe(nsol->nbrt, dev_id);
    if (nbr_e) {
        nbr_e->status = NBR_ACTIVE;
        nbr_table_unlock(nsol->nbrt);
    } else {
        nbr_table_unlock(nsol->nbrt);
        device_id_t *dev_id_copy = alloc_device_id(hdr->src_devid);
        metric_t *m = alloc_metric(1);
        nbr_e = alloc_nbr_entry(dev_id_copy, m, iface);
        nbr_table_add(nsol->nbrt, nbr_e);
    }
}

static void __fwd_topo_report(nso_layer_t *nsol, packet_t *pkt) {
    pthread_mutex_lock(&nsol->state_lock);
    int ret = nsol->dev_state == NRG5_CONNECTED;
    pthread_mutex_unlock(&nsol->state_lock);
    if (!ret) {
        LOG_DEBUG("drop received topo report!\n");
    } else {
        //nso_layer_fwd(pkt);
        send_pkt_to_vnf(son.vson_fd, pkt);
    }
}

int son_process_rx(packet_t *pkt, l2addr_t *src, l2addr_t *dst, nso_if_t *iface) {
    uint8_t type = *(pkt->data + sizeof(struct nsohdr));
    nso_layer_t *nsol = &nso_layer;
    switch(type){
        case NSO_SON_MSG_RT:
            __route_update(nsol, pkt, iface);
            break;
        case NSO_SON_MSG_NBR:
            __neighbor_maintain(nsol, pkt, src, dst, iface);
            break;
        case NSO_SON_MSG_TOPO:
            __fwd_topo_report(nsol, pkt);
            break;
        default:
            LOG_DEBUG("unknown son message!\n");
            return -1;
    }
    return 0;
}
