#include "api.h"
#include "nso.h"

int nso_send(uint8_t *buf, int size, nso_addr_t *dest, uint16_t proto) {
    return nso_layer_send(buf, size, (device_id_t*)dest, proto);
}

int nso_receive(uint8_t *buf, int size, nso_addr_t *src, nso_addr_t *dst, uint16_t *proto) {
    return nso_layer_recv(buf, size, (device_id_t*)src, (device_id_t*)dst, proto);
}

int nso_get_mtu() {
    return nso_layer_get_mtu();
}

int nso_get_device_id(nso_addr_t *dev_id){
    return nso_layer_get_device_id((device_id_t*)dev_id);
}

int nso_is_connected(){
    return nso_layer_is_connected();
}
