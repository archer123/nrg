import socketserver
import sys
from socketserver import StreamRequestHandler as Tcp
import struct, json


NSOHDR_LEN = 20

class myTCP(Tcp):

    def handle(self):
        while True:
            nsohdr = self.request.recv(NSOHDR_LEN) #hdr
            src_id, dst_id, proto, len_ver = struct.unpack("!8s8sHH", nsohdr)
            msglen = len_ver & 0x3fff
            msglen -= NSOHDR_LEN
            print((src_id), (dst_id), proto, len_ver)
            msg = self.request.recv(msglen)
            print(msg)

addr = ("", int(sys.argv[1]))
server = socketserver.TCPServer(addr, myTCP)
server.serve_forever()
