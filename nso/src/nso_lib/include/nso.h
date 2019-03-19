#ifndef __NSO_H__
#define __NSO_H__

#include "arp.h"
#include "fwding.h"
#include "nso_if.h"
#include "neighbor.h"
#include "nso_common.h"
#include <pthread.h>
#include "packet_queue.h"
#include <arpa/inet.h>

#define NSO_MAX_SUPPORTED_IFACES 10
#define NSO_DEFAULT_AGING_PERIOD_MS 10000
#define NSO_DEFAULT_TIMEOUT_MS NSO_DEFAULT_AGING_PERIOD_MS
#define NSO_DEFAULT_SON_REPORT_PERIOD_MS 1000

typedef enum {
    NRG5_UNREG = 0,
    NRG5_REG = 1,
    NRG5_CONNECTED = 2,
}device_status_e;

typedef struct {
    //interface list
    nso_if_t *ifaces[NSO_MAX_SUPPORTED_IFACES];
    int ifaces_nb;

    //arp table
    arp_table_t *arpt;

    //fwding table
    fwd_table_t *son_fwdt;
    fwd_table_t *local_fwdt;

    //neighbor table
    nbr_table_t *nbrt;

    //gw device id
    device_id_t *gw_id;

    //device status
    //TODO: change to atomic variable, instead of using mutex for performance reason
    device_status_e dev_state;
    pthread_mutex_t state_lock;
    pthread_cond_t state_signal;

    //self id
    device_id_t *dev_id;

    //thread handle
    //rx thread: for receiving packets from ifaces
    //for each interface, we use a thread to poll packet
    pthread_t rx_pid[NSO_MAX_SUPPORTED_IFACES];

    //tx thread: for sending control plane packets to xMEC
    pthread_t tx_pid;
    //aging thread: for aging tables
    pthread_t aging_pid;

    int timeout_ms;
    int aging_period_ms;
    //son report period
    int sr_period_ms;
    //it is the maximum among MTUs of interfaces.
    //it is the L2 MTU. That is the length of nso header is inclusive
    int mtu;

    //TODO: change to atomic variable for thread-safety
    uint8_t battery;

    //data packet queue
    pq_t data_pq;
    pthread_mutex_t data_lock;
    pthread_cond_t data_signal;

    //aaa configuration
    int aaa_port;
    struct in_addr aaa_addr;
    uint8_t aaa_reg_cred[1024];
    int aaa_cred_size;

} nso_layer_t;

int nso_layer_run(char *config_file);
int nso_layer_stop();
int nso_layer_fwd(packet_t *pkt);

/*----------------   API for upper layer-------------------------------*/

int nso_layer_get_mtu();
int nso_layer_recv(uint8_t *buf, int size, device_id_t *src, device_id_t *dst, uint16_t *proto);
int nso_layer_send(uint8_t *buf, int size, device_id_t *dst, uint16_t proto);

int nso_layer_get_device_id(device_id_t *dev_id);
int nso_layer_is_connected();
#endif
