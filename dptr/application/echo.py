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
args = parser.parse_args()

HOST_ADDR = ""
HOST_PORT = args.port

class service(socketserver.BaseRequestHandler):

    def handle(self):

        while True:
            try:
                data = self.request.recv(1024)
                logging.debug(data)
                self.request.sendall(data)
            except Exception as e:
                print(e)
                break


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass

if __name__ == "__main__":
    ThreadedTCPServer.allow_reuse_addr = True
    server = ThreadedTCPServer((HOST_ADDR, HOST_PORT), service)
    server.serve_forever()
