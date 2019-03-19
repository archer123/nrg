#include "nso_aaa.h"
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include "nso.h"
#include <unistd.h>

extern nso_layer_t nso_layer;
aaa_data_t aaa_mod;

const uint8_t test_credentials[] = {
    0x01, 0x02, 0x03, 0x04,
    0x05, 0x06, 0x07, 0x08,
};

//as we don't have the AAA implemetation now, just use this simple code to simulate the behaviour of AAA
int aaa_get_credentials(uint8_t *in_buf, int in_size, uint8_t *out_buf, int *out_size) {
    int size = htonl(in_size);
    send(aaa_mod.sockfd, &size, 4, 0);
    send(aaa_mod.sockfd, in_buf, in_size, 0);
    //recv
    recv(aaa_mod.sockfd, out_size, 4, 0);
    *out_size = ntohl(*out_size);
    recv(aaa_mod.sockfd, out_buf, *out_size, 0);
    return 0;
}

void aaa_init() {
    nso_layer_t *nsol = &nso_layer;
    
    nsol->aaa_cred_size = 0;

    aaa_mod.sockfd = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in addr;

    addr.sin_family = AF_INET;
    addr.sin_port = htons(nsol->aaa_port);
    addr.sin_addr.s_addr = nsol->aaa_addr.s_addr;

    while(connect(aaa_mod.sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0)
        LOG_DEBUG("AAA module is not connectable!\n");
}

void aaa_uninit() {
    close(aaa_mod.sockfd);
}

