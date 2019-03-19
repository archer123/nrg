#ifndef __BS_UTIL_H__
#define __BS_UTIL_H__

#include <stdint.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include "log.h"
#include "packet.h"
#include "nso_packet.h"

//return sockfd
static int connect_vnf(char *ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    inet_aton(ip, &addr.sin_addr);
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        LOG_DEBUG("connect vnf %s,%d failed\n", ip, port);
        return -1;
    }
    return fd;
}

static int __fully_recv(int fd, uint8_t *buf, int size) {
    int ret, cnt = 0;
    while (cnt != size) {
        ret = recv(fd, buf + cnt, size - cnt, 0);
        if (ret < 0) {
            LOG_DEBUG("recv from vnf error!\n");
            return ret;
        }
        cnt += ret;
    }
    return cnt;
}

static packet_t* read_pkt_from_vnf(int sockfd, int size) {
    packet_t *pkt = alloc_packet(size);
    __fully_recv(sockfd, pkt->data, sizeof(struct nsohdr));

    struct nsohdr *hdr = (struct nsohdr*)pkt->data;
    hdr->len_ver = ntohs(hdr->len_ver);
    int len = hdr->length;
    hdr->len_ver = htons(hdr->len_ver);
	
    LOG_DEBUG("message len %d\n", len);

    __fully_recv(sockfd, pkt->data + sizeof(struct nsohdr), len - sizeof(struct nsohdr));

    pkt->byte_len = sizeof(struct nsohdr) + len;
    pkt->tail = pkt->data + pkt->byte_len;
    return pkt;
}

static int send_pkt_to_vnf(int sockfd, packet_t *pkt) {
    int ret, cnt = 0;
    while (cnt != pkt->byte_len) {
        ret = send(sockfd, pkt->data + cnt, pkt->byte_len - cnt, 0);
        if (ret < 0) {
            LOG_DEBUG("send to vnf failed!\n");
            return ret;
        }
        cnt += ret;
    }
    return cnt;
}

#endif
