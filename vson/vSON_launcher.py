import logging
logging.basicConfig(level=logging.DEBUG)
import sys, os
sys.path.append(os.path.abspath(os.path.join("..")))
from vSON_southbound import vson
from vSON_northbound import RESTfulAPI
import threading
import argparse
from vSON_graph import topo


### set log destination
# logging.basicConfig(filename="vSON.log", level=logging.DEBUG)


parser = argparse.ArgumentParser()
parser.add_argument("-p", "--port", required=True, dest="port_num", help="select on wich port to open the listener, default = 2904",
                  metavar="<port>", type=int)
parser.add_argument("--api_port", required=True, type=int, help="port for restful API server", default=5000)
args = parser.parse_args()


if __name__ == "__main__":


    port_num = args.port_num

    if not port_num:
        port_num = 2904

    while True:
        try:
            port_num = int(port_num)
            break
        except ValueError:
            print "vSON_listener:main: Invalid port number..."
            pass

    logging.info("Starting Southbound listener deamon...")
    t = threading.Thread(target=vson('', port_num).listen)
    t.setDaemon(True)
    t.start()

    logging.info("Starting Device Monitor deamon...")
    topo.start_device_monitor()


    logging.info("Starting Nortbound Rest Interface...")
    RESTfulAPI(args.api_port)
