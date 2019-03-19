#include "wifi_ops.h"
#include "log.h"
#include <arpa/inet.h>
#include <linux/if_packet.h>
#include <string.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <net/if.h>
#include <unistd.h>

/* interface handle will be returned by handle argument*/
int wifi_open(char *name, nic_handle_t **ret_handle) {

    wifi_handle_t *handle = malloc(sizeof(wifi_handle_t));

    //open rawsocket for nic "name"
    handle->sockfd = socket(AF_PACKET, SOCK_RAW, htons(ETH_P_ALL));
    if (handle->sockfd == -1) {
        LOG_DEBUG("opening socket for %s is failed\n", name);
        goto err_ret_noclose;
    }

    //set promisc mode here
    struct ifreq flags;
    strcpy(flags.ifr_name, name);
    ioctl(handle->sockfd, SIOCGIFFLAGS, &flags);
    flags.ifr_flags |= IFF_PROMISC;
    ioctl(handle->sockfd, SIOCSIFFLAGS, &flags);

    //set reuseaddr option for socket
    int opt = 1;
    if (setsockopt(handle->sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
        LOG_DEBUG("set reuse addr failed for socket of %s\n", name);
        goto err_ret;
    }

    //bind socket with nic
    struct ifreq if_idx;
    memset(&if_idx, 0, sizeof(if_idx));
    strcpy(if_idx.ifr_name, name);
    if (ioctl(handle->sockfd, SIOCGIFINDEX, &if_idx) < 0) {
        LOG_DEBUG("unable to get index of nic %s\n", name);
        goto err_ret;
    }

    struct sockaddr_ll sock_addr;
    memset(&sock_addr, 0, sizeof(sock_addr));
    sock_addr.sll_family = AF_PACKET;
    sock_addr.sll_protocol = htons(ETH_P_ALL);
    sock_addr.sll_ifindex = if_idx.ifr_ifindex;

    if (bind(handle->sockfd, (const struct sockaddr*)&sock_addr, sizeof(sock_addr)) < 0) {
        LOG_DEBUG("unable to bind %s with its' socket\n", name);
        goto err_ret;
    }

    //set index of handle
    handle->if_index = if_idx.ifr_ifindex;

    //get mac of nic
    struct ifreq if_mac;
    memset(&if_mac, 0, sizeof(if_mac));
    strcpy(if_mac.ifr_name, name);
    if (ioctl(handle->sockfd, SIOCGIFHWADDR, &if_mac) < 0) {
        LOG_DEBUG("unable to get mac addr of %s\n", name);
        goto err_ret;
    }

    //set mac of handle
    memcpy(handle->if_mac, (char*)&if_mac.ifr_hwaddr.sa_data, ETH_ALEN);

    //get mtu
    if (ioctl(handle->sockfd, SIOCGIFMTU, &if_mac) < 0) {
        LOG_DEBUG("unable to get mtu if %s\n", name);
        goto err_ret;
    }
    handle->if_mtu = if_mac.ifr_mtu;

    *ret_handle = (nic_handle_t*)handle;
    return 0;


err_ret:
    close(handle->sockfd);
err_ret_noclose:
    free(handle);
    return -1;
}

int wifi_close(nic_handle_t *handle) {
    wifi_handle_t *wifi_handle = (wifi_handle_t*)handle;
    close(wifi_handle->sockfd);
    free(wifi_handle);
    return 0;
}

int wifi_send(nic_handle_t *handle, packet_t *pkt, l2addr_t *dst) {
    wifi_handle_t *wifi_handle = (wifi_handle_t*)handle;
    struct sockaddr_ll sock_addr;
    memset(&sock_addr, 0, sizeof(sock_addr));
    sock_addr.sll_family = AF_PACKET;
    sock_addr.sll_ifindex = wifi_handle->if_index;
    sock_addr.sll_halen = ETH_ALEN;

    if (pkt->data - pkt->buf < sizeof(struct ethhdr)) {
        LOG_DEBUG("headroom is not enough\n");
        return -1;
    }
    
    //add ethernet header
    struct ethhdr *eth = (struct ethhdr*)(pkt->data - sizeof(struct ethhdr));
    memcpy(eth->h_source, wifi_handle->if_mac, ETH_ALEN);
    memcpy(eth->h_dest, dst->addr, ETH_ALEN);
    eth->h_proto = htons(ETH_TYPE_NSO);
    pkt->data = (uint8_t*)eth;
    pkt->byte_len += sizeof(struct ethhdr);

    memcpy(sock_addr.sll_addr, eth->h_dest, ETH_ALEN);
    
    if (sendto(wifi_handle->sockfd, pkt->data, pkt->byte_len, 0, (struct sockaddr*)&sock_addr, sizeof(sock_addr)) < 0) {
        LOG_DEBUG("send packet failed\n");
        return -1;
    }
    return 0;
}

int wifi_broadcast(nic_handle_t *handle, packet_t *pkt) {
    uint8_t bc_mac[] = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
    l2addr_t *addr = alloc_l2addr(ETH_ALEN, bc_mac);
    int ret = wifi_send(handle, pkt, addr);
    free_l2addr(addr);
    return ret;
}

int wifi_receive(nic_handle_t *handle, packet_t *pkt, l2addr_t **src, l2addr_t **dst) {
    wifi_handle_t *wifi_handle = (wifi_handle_t*)handle;
    if (pkt->data - pkt->buf < sizeof(struct ethhdr)) {
        LOG_DEBUG("headroom of packet is not enough!\n");
        return -1;
    }
    pkt->data -= sizeof(struct ethhdr);
    int numbytes = recvfrom(wifi_handle->sockfd, pkt->data, pkt->size + sizeof(struct ethhdr), 0, NULL, NULL);
    if (numbytes <= 0) {
        LOG_DEBUG("unable to receive a valid packet\n");
        goto err_ret;
    }

    //extract ethernet header
    struct ethhdr *eth = (struct ethhdr*)pkt->data;
    if(ntohs(eth->h_proto) != ETH_TYPE_NSO) {
        memset(pkt->data, 0, numbytes);
        goto err_ret;
    } 

    *src = alloc_l2addr(ETH_ALEN, eth->h_source);
    *dst = alloc_l2addr(ETH_ALEN, eth->h_dest);
    pkt->data += sizeof(struct ethhdr);
    pkt->byte_len = numbytes - sizeof(struct ethhdr);
    pkt->tail = pkt->data + pkt->byte_len;
    return 0;

err_ret:
    pkt->data += sizeof(struct ethhdr);
    return -1;
}

int wifi_get_info(nic_handle_t *handle, nic_info_t *info) {
    wifi_handle_t *wifi_hdl = (wifi_handle_t*)handle;
    info->mtu = wifi_hdl->if_mtu;
    return 0;
}
