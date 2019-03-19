#!/bin/bash

USAGE="Usage: <.sh> -i interface -f frequency --ssid ssid"
OPTS=`getopt -o i:f: --long ssid: -n '$USAGE' -- "$@"`

if [ $? != 0 ] ; then
    echo $USAGE
    exit 1
fi

eval set -- $OPTS


INTF=""
FREQ=""
SSID=""

while true; do
    case "$1" in
        -i) INTF=$2; shift 2;;
        -f) FREQ=$2; shift 2;;
        --ssid) SSID=$2; shift 2;;
        --) shift; break;;
        *) echo $USAGE; exit 1;;
    esac
done

echo $INTF $FREQ"MHz" $SSID

iw dev $INTF set type ibss
ifconfig $INTF 0 up
iw dev $INTF ibss join $SSID $FREQ

iwconfig $INTF
