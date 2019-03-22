import sys, os
sys.path.append(os.path.abspath(os.path.join("..")))
from vSON_graph import *
import socket
import threading
import struct
import time


class vson(object):

    def __init__(self, host, port):
        ### open the request listener n specified port
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        #self.sock.settimeout(0.001)  # allow monitor to lock by pausing in between two reading

        logging.info("Listener is started on port " + str(port))




    def listen(self):
        self.sock.listen(5)
        while True:

            try:
                client, address = self.sock.accept()
                logging.info("Receive connection from " + str(address))

                t = threading.Thread(target=self.__listen_to_bs, args=(client, address))
                t.setDaemon(True)
                t.start()

                logging.debug("Opening a threaded socket for client: " + str(address))
            except:
                pass



    def __generate_unsupported_msgtype_err(self, src, dst):
        # 8B src, 8B dst, 2B proto, 2B len, 1B MSG_TYPE, 1B ANSWER
        msg = struct.pack("!QQHHB", src, dst, PROTO.SON, NSO_HDR_LEN + 1, TSDMSG.UNSUPPORTED_MSGTYPE_ERROR)
        return msg


    def __process_topo_repo(self, src, dst, msg):
        logging.info("Processing TOPOLOGY REPORT: {}".format(" ".join("{:02X}".format(ord(c)) for c in msg)))
        #TODO: push data in topology

        if not topo.isNodeRegistered(src):
            logging.warning('Node {:X} not registered'.format(src))
        intfs = []
        nbrs = []


        # ## message format:
        # 1B    :battery
        # 1B    :number of interfaces(N)
        # Nx2B  :interface (index[1], quality[1])
        # 2B    :number on neighbours(M)
        # Mx10B  :Neighbour (ID[8], quality[1], interface[1])

        batt, N, rest = struct.unpack("!BB{}s".format(len(msg)-2), msg)
        logging.debug("batt: {:X}, N: {}, rest: {}".format(batt, N, " ".join("{:02X}".format(ord(c)) for c in rest)))
        if N:
            itfs_raw, M, nbrs_raw = struct.unpack("!{}sH{}s".format( (N*2), (len(rest)-2-(N*2)) ), rest)
            logging.debug("intfs: {}, M: {}, rest: {}". format(" ".join("{:02X}".format(ord(c)) for c in itfs_raw), M, " ".join("{:02X}".format(ord(c)) for c in nbrs_raw)))
            for i in range (0, N):
                idx, q, itfs_raw = struct.unpack("!BB{}s".format((N-i-1)*2), itfs_raw)
                logging.debug("idx: {:X}, {}; rest: {}".format(idx, q, " ".join("{:02X}".format(ord(c)) for c in itfs_raw)))
                intfs.append(Interface(idx, q))

        if M:
            for i in range (0, M):
                uuid, lq, intf_idx, nbrs_raw = struct.unpack("!QBB{}s".format((M-1-i)*10), nbrs_raw)
                logging.debug("nbr: {:X}, {}, {:X}; rest: {}".format(uuid, lq, intf_idx, " ".join("{:02X}".format(ord(c)) for c in nbrs_raw)))
                nbrs.append(Link(beginID=src, endID=uuid, link_q=lq, intf_idx=intf_idx))

        logging.debug("structure unpacked:\n"
                      "batt: {}\n "
                      "#intfs: {}\n "
                      "intfs: {}\n "
                      "#nbrs: {}\n "
                      "nbrs: {}".format(batt,
                                        N,
                                        intfs,
                                        M,
                                        nbrs)
                      )

        status, node = topo.put_node_info(src, battery=batt, intfs=intfs, nbrs=nbrs)

        return None
        #return self.__gen_topo_update_reply(src, dst, ANSWER.SUCCESS if status==STATUS.SUCCESS else ANSWER.FAIL)

    def __gen_topo_update_reply(self, dst, src, answer):
        # 8B src, 8B dst, 2B proto, 2B len, 1B MSG_TYPE, 1B ANSWER
        msg = struct.pack("!QQHHBB", src, dst, PROTO.SON, NSO_HDR_LEN + 2, SONMSG.TOPO_REPO_REPLY, answer)
        return msg


    def __process_SON(self, src, dst, sock, pl_size):
        payload = sock.recv(pl_size)
        logging.info("Processing SON message: {}".format(" ".join("{:02X}".format(ord(c)) for c in payload)))

        msg_typ, msg = struct.unpack("!B{}s".format(pl_size-1), payload)
        logging.info("Received message type [{:02X}]".format(msg_typ))
        lock.acquire()
        try:
            if msg_typ == SONMSG.TOPO_REPO:
                reply = self.__process_topo_repo(src, dst, msg)
            else:
                reply = self.__generate_unsupported_msgtype_err(src, dst)

            return reply
        finally:
            lock.release()


    def __receive_nso_hdr(self, sock):
        nso_hdr = sock.recv(NSO_HDR_LEN)
        if len(nso_hdr) == NSO_HDR_LEN:
            logging.debug("Unpacking header {}".format(" ".join("{:02X}".format(ord(c)) for c in nso_hdr)))
            try:
                src, dst, proto, length = struct.unpack("!QQHH", nso_hdr)
                logging.debug("NSO_HDR print: {}".format(src, dst, proto,length))
            except struct.error:

                logging.debug("Unknown NSO_HDR print: {}".format(src," ", dst," ", proto," ", length))

                raise NSOException(STATUS.SOUTHBOUND_NOT_NSO, 'received a# non NSO packet')

            return (src, dst, proto, length, nso_hdr)

        else:
            logging.debug("Length incorrect NSO_HDR print: {}".format(len(nso_hdr)))

            raise NSOException(STATUS.SOUTHBOUND_NOT_NSO, 'received a non NSO packet')






    def __listen_to_bs(self, client, address):

        while True:

            try:
                logging.debug('CONNECTION: receiving...')
                src, dst, proto, length, hdr_str = self.__receive_nso_hdr(client)

                if src and dst and length and hdr_str:
                    logging.info("Received packet protocol [{:02X}], payload size = {}".format(proto, length-NSO_HDR_LEN))

                    # FIXME: extend message discriminator
                    if proto == PROTO.SON:
                        response = self.__process_SON(src, dst, client, length-NSO_HDR_LEN)
                    else:
                        response = self.__generate_unsupported_msgtype_err(src, dst)


                    #if not response:
                    #    response = "none"

                    if response:
                        client.send(response)
                        logging.debug("Replying to " + str(address) + " with " + "{}".format(" ".join("{:02X}".format(ord(c)) for c in response)))

            except NSOException as x:
                logging.error(x.msg)
                client.send('error')
                pass


    def __device_is_authenticated(self, uuid, credentials):
        # FIXME : connect to vAAA_simulation service
        response = False

        while True:
            response = raw_input(
                "Incoming registration request from {}, cred: {}. Do you want to accept it? (yes/no) ").format(uuid,
                                                                                                               credentials)
            if (response == "yes") or (response == "no"):
                break
        if response == "yes":
            return True
        else:
            return False
