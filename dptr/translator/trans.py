#!/usr/bin/env python3

import socketserver, sys, os
import struct
import logging
import threading
import argparse
import socket
from threading import Thread

sys.path.append(os.path.abspath(os.path.join("..", "..")))
from common.Def import *

logging.basicConfig(level=logging.DEBUG)
parser = argparse.ArgumentParser()
parser.add_argument("-p", "--port", type=int, required=True, help="translator will be running on which port")
parser.add_argument("--app_addr", required=True, help="application server's IP addr, e.g. 192.168.1.1")
parser.add_argument("--app_port", type=int, required=True, help="application server's TCP port, e.g. 8080")
args = parser.parse_args()

APP_ADDR = args.app_addr
APP_PORT = args.app_port
HOST_ADDR = ""
HOST_PORT = args.port

class _NsoInfo(object):

    def __init__(self, src_id, gw_id, proto):
        """
        src_id, gw_id = 8B 'str'
        proto = 2B 'short'
        """
        self.src_id = src_id
        self.gw_id = gw_id
        self.proto = proto

    def __eq__(self, other):
        if isinstance(other, _NsoInfo):
            raise TypeError("{} is not a _NsoInfo Object!".format(other))
        return self.src_id == other.src_id and self.gw_id == other.gw_id and self.proto == other.proto


class _MapEntry(object):

    def __init__(self, src_id, gw_id, proto, sock):
        self.nso_info = _NsoInfo(src_id, gw_id, proto)
        self.sock = sock


class _Nso2TcpMap(object):

    def __init__(self):
        #device id to map object
        #sockfd id to map object
        self.__nso_map = {}

    def add_entry(self, src_id, gw_id, proto, sock):
        entry = _MapEntry(src_id, gw_id, proto, sock)
        self.__nso_map[src_id] = entry
        return entry

    def lookup_from_src_id(self, src_id):
        return self.__nso_map.get(src_id)



class service(socketserver.BaseRequestHandler):

    def __recv_nso_msg(self):
        #recv header
        hdr = self.request.recv(NSO_HDR_LEN)
        if not hdr:
            raise Exception("socket recv error")
        src_id, dst_id, proto, len_ver = struct.unpack("!8s8sHH", hdr)
        data_len = len_ver & 0x3fff
        #recv data
        data = self.request.recv(data_len)
        if not data:
            raise Exception("socket recv error")
        #return tuple
        return (src_id, dst_id, proto, data)

    def handle(self):

        def __rx_from_app_server(map_entry):

            def __pack_nso_packet(nso_info, data):
                len_ver = (NSO_HDR_LEN + len(data)) & 0x3fff
                return struct.pack("!8s8sHH{}s".format(len(data)), nso_info.gw_id, nso_info.src_id, nso_info.proto, len_ver, data)

            nso_info = map_entry.nso_info
            sock = map_entry.sock

            while True:
                app_data = sock.recv(1024)
                if not app_data:
                    logging.debug("socket recv from server error")
                    break
                nso_data = __pack_nso_packet(nso_info, app_data)
                logging.debug("forward message to devices: {}".format(nso_data))
                self.request.sendall(nso_data)

        nso2tcp_map = _Nso2TcpMap()
        logging.info("{} connect!".format(self.client_address))

        while True:
            try:
                src_id, gw_id, proto, data = self.__recv_nso_msg()
                logging.debug("receive message from devices: src_id {}, dst_id {}, proto {}, data {}".format(src_id, gw_id, hex(proto), data))

                entry = nso2tcp_map.lookup_from_src_id(src_id)
                if not entry:
                    #connect to the application server, and launch a thread to process rx of that client
                    logging.info("get a new device!")
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect((APP_ADDR, APP_PORT))
                    entry = nso2tcp_map.add_entry(src_id, gw_id, proto, sock)

                    threading.Thread(target=__rx_from_app_server, args=(entry,)).start()

                #forward all data to application server
                entry.sock.sendall(data)
            except Exception as e:
                logging.debug("rx thread error!")
                logging.debug(e)
                break


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass

if __name__ == "__main__":
    ThreadedTCPServer.allow_reuse_address = True
    server = ThreadedTCPServer((HOST_ADDR, HOST_PORT), service)
    server.serve_forever()
