#!/bin/bash

RT_PATH=/home/pi/nrg5
NRG5_TOPDIR=$RT_PATH/nrg-5-sorbonne
AAA_DIR=$RT_PATH/vBCP-client
BS=$NRG5_TOPDIR/bs
NODEJS=$NRG5_TOPDIR/websocket-io

$BS/script/wifi_adhoc.sh -i wlan0 -f 2412 --ssid nrg5sorbonne &> $RT_PATH/nrg5-log/intf1.log
$BS/script/wifi_adhoc.sh -i wlan1 -f 2412 --ssid nrg5sorbonne &> $RT_PATH/nrg5-log/intf2.log

echo "open NRG5 interfaces"
sleep 5

pushd $AAA_DIR/examples/aaa_module/
python3 example.py &> $RT_PATH/nrg5-log/aaa.log &
popd

echo "open AAA module"
sleep 1

pushd $NRG5_TOPDIR/vtsd/
python vTSD_launcher.py -p 1234 --vson_addr 127.0.0.1 --vson_port 5000 &> $RT_PATH/nrg5-log/vtsd.log &
popd

echo "start vTSD"
sleep 1

pushd $NRG5_TOPDIR/vson/
python vSON_launcher.py -p 1235 --api_port 5000 &> $RT_PATH/nrg5-log/vson.log &
popd

echo "start vSON"
sleep 1

pushd $NODEJS
nodejs index.js &> $RT_PATH/nrg5-log/nodejs.log &
popd

echo "start nodejs server"
sleep 1

pushd $NRG5_TOPDIR/dptr/application
./power_meter.py -p 8080 --web_addr 127.0.0.1 --web_port 3000 &> $RT_PATH/nrg5-log/app.log &
popd

echo "start application server"
sleep 1

pushd $NRG5_TOPDIR/dptr/translator
./trans.py -p 1236 --app_addr 127.0.0.1 --app_port 8080 &> $RT_PATH/nrg5-log/dptr.log &
popd

echo "start DPTR"
sleep 1

pushd $BS/src/bs_app
./bs_app ../../config/config_example &> $RT_PATH/nrg5-log/bs.log &
popd

echo "start base station"

