#!/bin/bash

RT_PATH=/home/pi
NRG5_TOPDIR=$RT_PATH/nrg-5-sorbonne
AAA_DIR=$RT_PATH/vBCP-client
NSO=$NRG5_TOPDIR/nso

$NSO/script/wifi_adhoc.sh -i wlan0 -f 2412 --ssid nrg5sorbonne &> $RT_PATH/nrg5-log/intf1.log

echo "open NRG5 interface"
sleep 5

pushd $AAA_DIR/examples/aaa_module/
python3 example.py &> $RT_PATH/nrg5-log/aaa.log &
popd

echo "open AAA module"
sleep 1

#pushd $NSO/src/app_example/power_meter
#./power_meter ../../../config/config_example &> $RT_PATH/nrg5-log/nso.log &
#popd

echo "start nso device"
sleep 1
