#include "nso.h"
#include <string.h>


int main(int argc, char **argv){
    if (argc != 2) {
        printf("argv[1] = config file!\n");
        return -1;
    }
    nso_layer_run(argv[1]);
    while(1);
    return 0;
}
