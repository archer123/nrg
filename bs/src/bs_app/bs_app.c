#include "nso.h"
#include "api.h"
#include <string.h>
#include <wiringPi.h>
#include <signal.h>

#define LED_PIN 28

static void turn_on_led() {
    wiringPiSetup();
    pinMode(LED_PIN, OUTPUT);
    digitalWrite(LED_PIN, HIGH);
}

static void turn_off_led(int signo) {
    digitalWrite(LED_PIN, LOW);
    exit(-1);
}


int main(int argc, char **argv){
    if (argc != 2) {
        printf("argv[1] = config file!\n");
        return -1;
    }
    signal(SIGINT, turn_off_led);
    signal(SIGQUIT, turn_off_led);
    signal(SIGPIPE, turn_off_led);
    nso_layer_run(argv[1]);
    while (!nso_is_connected());
    turn_on_led();
    while (1);
    return 0;
}
