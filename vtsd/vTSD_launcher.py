import socket
import threading
import sys, os
import json, struct
import logging
import requests
sys.path.append(os.path.abspath(os.path.join("..")))
from common.Def import *


from common.Def import *
import argparse


### set log destination
#logging.basicConfig(filename="vTSD.log", level=logging.DEBUG)
parser = argparse.ArgumentParser()
parser.add_argument("-p", "--port", required=True, type=int, help="vtsd port")
parser.add_argument("--vson_addr", required=True, help="vson addr")
parser.add_argument("--vson_port", required=True, type=int, help="vson port")
args = parser.parse_args()


class vtsd(object):


    def __init__(self, host, port):
        ### open the request listener n specified port
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))


    def listen(self):
        self.sock.listen(5)
        while True:
            client, address = self.sock.accept()
            logging.info("Receive connection from " + str(address))

            t = threading.Thread(target=self.__listen_to_BS, args=(client, address))
            t.setDaemon(True)
            t.start()
            #logging.debug("Opening a threaded socket for client: " + str(address))



    def __generate_device_reg_reply(self, dst, src, answer):
        # 8B src, 8B dst, 2B proto, 2B len, 1B MSG_TYPE, 1B ANSWER
        msg = struct.pack("!QQHHBB", src, dst, PROTO.TSD, NSO_HDR_LEN + 2, TSDMSG.DEV_REG_REPLY, answer)
        return msg


    def __generate_bs_reg_reply(self, dst, src, answer):
        # 8B src, 8B dst, 2B proto, 2B len, 1B MSG_TYPE, 1B ANSWER
        msg = struct.pack("!QQHHBB", src, dst, PROTO.TSD, NSO_HDR_LEN + 2, TSDMSG.BS_REG_REPLY, answer)
        return msg



    def __generate_unsupported_msgtype_err(self, src, dst):
        # 8B src, 8B dst, 2B proto, 2B len, 1B MSG_TYPE, 1B ANSWER
        msg = struct.pack("!QQHHB", src, dst, PROTO.TSD, NSO_HDR_LEN + 1, TSDMSG.UNSUPPORTED_MSGTYPE_ERROR)
        return msg


    def __notify_vSON(self, ID, jdata, bs=False):

        assert isinstance(bs, bool)
        url = 'http://{}:{}/'.format(args.vson_addr, args.vson_port)
        newID = hex(ID)
        payload = {
            'ID': newID,
            'bs': bs,
            'description': jdata["message"],
            'signature': jdata["signature"],
            'registered': True
        }

        return requests.post(url + 'topology/nodes/{}'.format(newID), json=payload)


    def __process_dev_reg(self, src, dst, req):
        logging.debug("Processing DEV_REG message {}".format(req))


        try:
            jdata = json.loads(req)
        except:
            logging.warning("is not json data")
            return None

        #logging.debug(jdata)

        #logging.debug("Chechink auth...")
        # response = requests.post(vAAA_URL, json=msg)

        response = self.__device_is_authenticated((jdata["message"]), (jdata["signature"]))

        if response == 200:
            logging.info("Authentication {}SUCCEED{}".format(frm.OKGREEN, frm.ENDC))

            logging.info("Reporting to vSON node {}".format(hex(src)))
            vSONresponse = self.__notify_vSON(src, jdata)
            if vSONresponse.status_code == 200:

                logging.info("vSON Registration {}SUCCEED{}".format(frm.OKGREEN, frm.ENDC))
                return self.__generate_device_reg_reply(src, dst, ANSWER.SUCCESS)

            else:
                logging.error(repr(vSONresponse.status_code) + ' ' + repr(vSONresponse.content))
                return None

        logging.info("Authentication {}FAILED{}".format(frm.FAIL, frm.ENDC))
        return self.__generate_device_reg_reply(src, dst, ANSWER.FAIL)




    def __process_bs_reg(self, src, dst, msg):
        logging.debug("Received BS_REG message: {}".format(msg))

        jdata = json.loads(msg)

        #logging.debug("Chechink auth...")
        # response = requests.post(vAAA_URL, json=msg)
        response = self.__device_is_authenticated((jdata[u"message"]), (jdata[u"signature"]), bs=True)

        if response == 200:
            logging.info("Authentication {}SUCCEED{}".format(frm.OKGREEN, frm.ENDC))
            logging.info("notify vSON!\n")

            vSONresponse = self.__notify_vSON(src, jdata, bs=True)
            if vSONresponse.status_code == 200:

                logging.info("vSON Registration {}SUCCEED{}".format(frm.OKGREEN, frm.ENDC))
                return self.__generate_bs_reg_reply(src, dst, ANSWER.SUCCESS)

            else:
                logging.error(repr(vSONresponse.status_code) + ' ' + repr(vSONresponse.content))
                logging.error("device already registered!\n")
                return None

        else:
            reply = self.__generate_bs_reg_reply(src, dst, ANSWER.FAIL)
            return reply


    def __process_TSD(self, src, dst, sock, pl_size):
        payload = sock.recv(pl_size)
        #logging.debug("Processing TSD message: {}".format(" ".join("{:02x}".format(ord(c)) for c in payload)))

        msg_typ, msg = struct.unpack("!B{}s".format(pl_size-1), payload)
        logging.debug("Received message type [{:02x}]".format(msg_typ))

        if msg_typ == TSDMSG.DEV_REG:
            reply = self.__process_dev_reg(src, dst, msg)
        elif msg_typ == TSDMSG.BS_REG:
            reply = self.__process_bs_reg(src, dst, msg)
        else:
            reply = self.__generate_unsupported_msgtype_err(src, dst)

        return reply

    def __receive_nso_hdr(self, sock):
        nso_hdr = sock.recv(NSO_HDR_LEN)
        if nso_hdr:
            logging.debug("Unpacking header {}".format(" ".join("{:02x}".format(ord(c)) for c in nso_hdr)))
            try:
                src, dst, proto, length = struct.unpack("!QQHH", nso_hdr)
            except struct.error:
                raise NSOException(STATUS.SOUTHBOUND_NOT_NSO, 'received a non NSO packet')

            return (src, dst, proto, length, nso_hdr)

        else:
            raise NSOException(STATUS.SOUTHBOUND_NOT_NSO, 'received a non NSO packet')

    def __listen_to_BS(self, client, address):

        while True:


            try:

                src, dst, proto, length, hdr_str = self.__receive_nso_hdr(client)

                logging.debug("Received packet protocol [{:02x}], payload size = {}".format(proto, length - NSO_HDR_LEN))
                # FIXME: extend message discriminator
                if proto == PROTO.TSD:
                    response = self.__process_TSD(src, dst, client, length-NSO_HDR_LEN if length else 0)
                elif proto == PROTO.SU2IP:
                    logging.debug("{}Received Data Packet: {}".format(frm.WARNING, frm.ENDC))
                    response = ""
                else:
                    response = self.__generate_unsupported_msgtype_err(src, dst)

                #if not response:
                #   response = ('none')

                if response:
                    client.sendall(response)
                    logging.debug("Replying to " + str(address) + " with " + "{}".format(" ".join("{:02x}".format(ord(c)) for c in response)))

            except NSOException as x:
                if x.code == STATUS.SOUTHBOUND_NOT_NSO:
                    # logging.warning('Received not NSO packet from {}'.format(address))
                    # logging.error(x.msg)
                    pass

                if x.code == STATUS.UNRECOGNIZED_BS:
                    logging.warning('Base Station not Authenticated, closing listener...')
                    return



    def __device_is_authenticated(self, uuid, credentials, bs=False):  # FIXME : connect to vAAA_simulation service
        #FIXME: @WARNING THIS IS JUST FOR TEST, BEACAUSE WE DONT HAVE THE FUCKING VAAA
        return 200

        response = False

        while True:
            response = raw_input(
                "Incoming registration request from {} {}. Do you want to accept it? (y/n) ".format('BASE STATION' if bs else '', uuid.encode('utf-8')))
            if (response == "y") or (response == "n"):
                break

        if response == "y":
            return 200
        else:
            return 500



if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)
    port_num = args.port

    if not port_num:
        port_num = 2311

    while True:
        try:
            port_num = int(port_num)
            break
        except ValueError:
            print "vTSD_listener:main: Invalid port number. Abort..."
            exit(1)

    logging.info('Starting Southbound listener on localhost:{}'.format(port_num))
    vtsd('', port_num).listen()
