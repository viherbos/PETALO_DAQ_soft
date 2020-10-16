import sys
import os
import json
import socket as sk
from bitarray import bitarray
from threading import Thread, Event
from collections import OrderedDict
import struct
from queue import Queue, Empty


def v_print(args):
    print(args),
    sys.stdout.flush()



class MESSAGE():
    def __init__(self):
        """
        COMM Protocol (wordlength=32):
        COMMAND + NUMBER OF PARAMs + PARAM1 + PARAM2 + ... + PARAMn

        COMMAND : ID (16 bits) + Destination (16 bits)
            PC -> DAQ Commands (bit 0 = 0)
            DAQ -> PC Commands (bit 0 =1)

            ID: Command Identification Code or ERROR Code
                ERROR Codes: -1 (ERR_BAD_PACKET)
                             -2 (ERR_INVALID_DESTINATION)
                             -3 (ERR_INVALID_COMMAND)
            Destination: DAQ board (PC-> DAQ // DAQ -> PC)

        NUMBER OF PARAMS: N_PARAMS = 1 -> PARAM1 = Status (OK=0,FAIL<0)

        """
        self.dict = OrderedDict([('command',0),('L1_id',0),('n_params',0),('params',0)])
        self.json = 0
        self.bits = bitarray()


    def __call__(self,in_data):

        self.dict = OrderedDict([('command',0),('L1_id',0),('n_params',0),('params',0)])
        self.json = 0
        self.bits = bitarray()

        switch = {"<class 'list'>": "encode",
                  "<class 'dict'>": "encode",
                  "<class 'bitarray.bitarray'>":  "decode"
                  }
        method_name = switch.get(str(type(in_data)),"decode")
        method      = getattr(self, method_name, lambda:"Invalid Data")
        return method(in_data)


    def encode(self,dict_in):
        """ Creates Dict, Json and Bitstream from {command,L1_id,args} dict
        """
        if type(dict_in)==type([]):
            command = dict_in[0]
            L1_id   = dict_in[1]
            args    = dict_in[2]
        elif type(dict_in)==type({'a':0,'b':0}):
            command = dict_in['command']
            L1_id   = dict_in['L1_id']
            args    = dict_in['params']

        # Error in args syntax
        try:
            len_args = len(args)
        except:
            args = [args]
            len_args = len(args)

        switch = { "SOFT_REG_W": {'code': 2, 'n_params':2},
                   "SOFT_REG_R": {'code': 4, 'n_params':1},
                   "HARD_REG_W": {'code': 6, 'n_params':2},
                   "HARD_REG_R": {'code': 8, 'n_params':1},
                   "PLL_REG_W" : {'code': 10, 'n_params':2},
                   "PLL_REG_R" : {'code': 12, 'n_params':1},
                   "I2C"       : {'code': 14, 'n_params':len(args)}
        }
        case = switch.get(command,{'code':-1, 'n_params':0})

        if (case['code'] == -1):
            print("Command Error")
            self.bits = -1
        else:
            if (case['n_params'] != len(args)):
                print("Parameter Error")
                self.bits = -1
            else:
                self.dict['command']  = case['code']
                self.dict['L1_id']    = L1_id
                self.dict['n_params'] = len(args)
                if (str(type(args[0]))=="<class 'int'>"):
                    self.dict['params']   = args
                else:
                    self.dict['params']   = [int(x,0) for x in args]
                self.translate()
                self.json = json.dumps(self.dict)

        return self.bits


    def decode(self,bit_stream):
        """ Decodes Bitstream into Dict and JSON
        """
        v = memoryview(bit_stream)
        # COMMAND_ID + DAQ_ID = 4 bytes
        # N_PARAMETERS        = 4 bytes

        command  = struct.unpack('<H',v[0:2])[0]
        L1_id    = struct.unpack('<H',v[2:4])[0]
        n_params = struct.unpack('<I',v[4:8])[0]
        format = '<'+str(n_params)+'I'
        params   = struct.unpack(format,v[8:])


        # Extract COMMAND
        switch = { 1 :{'name': 'CON_STATUS',   'n_params':2},
                   2 :{'name': 'SOFT_REG_W',   'n_params':2},
                   3 :{'name': 'SOFT_REG_W_r', 'n_params':1},
                   4 :{'name': 'SOFT_REG_R',   'n_params':1},
                   5 :{'name': 'SOFT_REG_R_r', 'n_params':2},
                   6 :{'name': 'HARD_REG_W',   'n_params':2},
                   7 :{'name': 'HARD_REG_W_r', 'n_params':1},
                   8 :{'name': 'HARD_REG_R',   'n_params':1},
                   9 :{'name': 'HARD_REG_R_r', 'n_params':2},
                   10:{'name': 'PLL_REG_W',    'n_params':2},
                   11:{'name': 'PLL_REG_W_r',  'n_params':1},
                   12:{'name': 'PLL_REG_R',    'n_params':1},
                   13:{'name': 'PLL_REG_R_r',  'n_params':2},
                   14:{'name': 'I2C',          'n_params':n_params},
                   15:{'name': 'I2C_r',        'n_params':n_params}
        }
        case = switch.get(command,{'name':'ERROR', 'n_params':0})

        self.dict['command']  = case['name']
        self.dict['L1_id']    = L1_id
        self.dict['n_params'] = n_params
        self.dict['params']   = [hex(x) for x in params]

        self.json = json.dumps(self.dict)

        return self.dict


    def translate(self):
        """ Auxiliary function:
            Translates Message in Dict into bit array (decimal base)
        """
        byte_frame = bytearray()

        for key in self.dict.keys():
            switch = {"command":'<H', "L1_id":'<H',"n_params":'<I',"params":'<I'}
            case = switch.get(key,0)
            if key=="params":
                set = self.dict[key]
            else:
                set = [self.dict[key]]
            for word in set:
                byte_frame.extend(bytearray(struct.pack(case,word)))

        #print(byte_frame)

        #self.bits.frombytes(str(byte_frame))

        self.bits = byte_frame

class LOGGER(Thread):

    """ PETALO DAQ LOGGER dumps RX thread output.
    """

    def __init__(self,upper_class,queue,stopper,in_out,pfile):
        super(LOGGER,self).__init__()
        self.uc = upper_class
        self.queue = queue
        self.stopper = stopper
        self.M = MESSAGE()
        if (in_out==0):
            self.chain = "<<"
        else:
            self.chain = ">>"
        self.pfile = pfile

    def run(self):
        while not self.stopper.is_set():
            try:
                self.item = self.queue.get(True,timeout=0.5)
                # Timeout should decrease computational load
            except Empty:
                pass
                # Wait for another timeout
            else:
                rx_data = self.item
                M_dict = self.M(rx_data)
                json_chain = json.dumps(M_dict)
                thing = "%s %s" % (self.chain,json_chain)
                v_print("\n" + thing +"\n>>")
                self.pfile.write("%s \n" % (json.dumps(M_dict)))
                self.pfile.flush()
                os.fsync(self.pfile.fileno())
                self.queue.task_done()

        print ("LOGGER is DEAD")

class SCK_TXRX(Thread):

    """ PETALO DAQ Transmission Socket.
        Designed to run as TXRX thread

        Parameters (General)
        'stopper'     : Flag to stop thread execution
        'queue'       : Queue to send data

        Parameters (taken from UC data)
        'ext_ip'      : DAQ IP Address
        'port'        : DAQ port
        'buffer_size' : Size of data to receive
    """

    def __init__(self,upper_class,tx_queue,rx_queue,stopper):
        super(SCK_TXRX,self).__init__()
        self.uc         = upper_class
        self.queue      = tx_queue
        self.out_queue  = rx_queue
        self.stopper    = stopper
        self.uc         = upper_class
        self.M          = MESSAGE()
        self.s = sk.socket(sk.AF_INET, sk.SOCK_STREAM)

        try:
            #print self.uc.daqd_cfg['ext_ip']
            self.s.connect((self.uc.data['ext_ip'],
                            int(self.uc.data['port'])))
            # ADD TIMEOUT Mechanism !!!!
            self.s.settimeout(5.0)
            data_r = self.M(bytearray(self.s.recv(int(self.uc.data['buffer_size']))))
            if (data_r['command'] != 'CON_STATUS'):
                v_print ('Communication Error (1)')
            elif ((data_r['command'] == 'CON_STATUS') and
                  (data_r['params'][0] == 1)):
                IP = sk.inet_ntoa(struct.pack("!I",data_r['params'][1]))
                v_print('\n<< Unsuccessful: DAQ is already connected to %s \n>> ' % (IP))
            else:
                v_print('\n<< Connection Stablished \n>> ')

        except sk.error as e:
            v_print(" \n Client couldn't open socket: %s \n>>" % e)


    def run(self):
        while not self.stopper.is_set():
            try:
                self.item = self.queue.get(True,timeout=0.25)
                # Timeout should decrease computational load
            except Empty:
                pass
                # Wait for another timeout
            else:
                try:
                    self.s.send(self.item)
                    self.queue.task_done()
                    # Get DAQ response
                    data_rx = self.s.recv(int(self.uc.data['buffer_size']))
                    self.out_queue.put(bytearray(data_rx))
                except:
                    v_print ('\n<< Communication Error - Timeout \n>> ')
        print ("TXRX SOCKET IS DEAD")




class SCK_init(object):
    def __init__(self,upper_class,socket):
        self.s   = socket
        self.uc  = upper_class
        self.M          = MESSAGE()
        try:
            #print self.uc.daqd_cfg['ext_ip']
            self.s.connect((self.uc.data['ext_ip'],
                            int(self.uc.data['port'])))
            # ADD TIMEOUT Mechanism !!!!
            self.s.settimeout(5.0)
            data_r = self.M(bytearray(self.s.recv(int(self.uc.data['buffer_size']))))
            if (data_r['command'] != 'CON_STATUS'):
                v_print ('Communication Error (1)')
            elif ((data_r['command'] == 'CON_STATUS') and
                  (data_r['params'][0] == 1)):
                v_print('\n<< DAQ is already connected to %d \n>> ' % (data_r['params'][1]))
            else:
                v_print('\n<< Connection Stablished \n>> ')

        except sk.error as e:
            v_print(" \n Client couldn't open socket: %s \n>>" % e)

class SCK_TX1(Thread):

    """ PETALO DAQ Transmission Socket.
        Designed to run as TX thread

        Parameters (General)
        'stopper'     : Flag to stop thread execution
        'queue'       : Queue to send data

        Parameters (taken from UC data)
        'ext_ip'      : DAQ IP Address
        'port'    : DAQ port
        'buffer_size' : Size of data to receive
    """

    def __init__(self,upper_class,tx_queue,stopper):
        super(SCK_TX1,self).__init__()
        self.uc         = upper_class
        self.queue      = tx_queue
        self.stopper    = stopper
        self.uc         = upper_class
        self.M          = MESSAGE()
        self.s = sk.socket(sk.AF_INET, sk.SOCK_STREAM)

        try:
            #print self.uc.daqd_cfg['ext_ip']
            self.s.connect((self.uc.data['ext_ip'],
                            int(self.uc.data['port'])))
            # ADD TIMEOUT Mechanism !!!!
            self.s.settimeout(5.0)
            data_r = self.M(bytearray(self.s.recv(int(self.uc.data['buffer_size']))))
            if (data_r['command'] != 'CON_STATUS'):
                v_print ('Communication Error (1)')
            elif ((data_r['command'] == 'CON_STATUS') and
                  (data_r['params'][0] == 1)):
                v_print('\n<< DAQ is already connected to %d \n>> ' % (data_r['params'][1]))
            else:
                v_print('\n<< Connection Stablished \n>> ')

        except sk.error as e:
            v_print(" \n Client couldn't open socket: %s \n>>" % e)



    def run(self):
        while not self.stopper.is_set():

            try:
                self.item = self.queue.get(True,timeout=0.25)
                # Timeout should decrease computational load
            except Empty:
                pass
                # Wait for another timeout
            else:
                try:
                    self.s.send(self.item)
                    self.queue.task_done()
                except:
                    v_print ('\n<< Communication Error - Timeout \n>> ')

        self.s.shutdown(sk.SHUT_WR)
        print ("TX1 SOCKET IS DEAD")

class SCK_RX1(Thread):

    """ PETALO DAQ Reception Socket.
        Designed to run as RX thread

        Parameters (General)
        'stopper'     : Flag to stop thread execution
        'queue'       : Queue to store received data

        Parameters (taken from UC data)
        'localhost'   : Host IP Address
        'port'        : port
        'buffer_size' : Size of data to receive
    """


    def __init__(self,upper_class,queue,stopper):
        super(SCK_RX1,self).__init__()
        self.uc = upper_class
        self.queue = queue
        self.stopper = stopper
        self.s = sk.socket(sk.AF_INET, sk.SOCK_STREAM)
        try:
            self.s.bind((self.uc.data['localhost'],
                         int(self.uc.data['port'])))
            self.s.listen(5)
        except sk.error as e:
            v_print("<< RX socket couldn't be opened: %s \n>> " % e)
            os._exit(1)


    def run(self):
        while not self.stopper.is_set():
            try:
                self.s.settimeout(5.0)
                self.conn, self.addr = self.s.accept()

            except sk.timeout:
                pass
            else:
                #print ("Connection Host/Address: %s  %s" % (self.uc.daqd_cfg['localhost'],
                #                                        self.addr))
                try:
                    self.s.settimeout(5.0)
                    # Ten seconds to receive the data
                    self.data = self.conn.recv(int(self.uc.data['buffer_size']))
                except:
                    v_print("<< Data not received by server \n>> ")
                    pass
                else:
                    self.queue.put(self.data)
                    # self.conn.send(json.dumps(BYE_MSG))
                    # Handshake Message
                    self.conn.close()
        self.s.close()
        print ("RX1 SOCKET IS DEAD")


if __name__ == "__main__":

    TX = MESSAGE()
    bits,json_tx = TX(["SOFT_REG_W",0,[23,16]])
    bits[15]=True

    RX = MESSAGE()
    dict,json_rx = RX(bits)
    print(dict)

    bits[14]=False
    RX2 = MESSAGE()
    dict,json_rx = RX2(bits)
    print(dict)

    TX = MESSAGE()
    bits,json_tx = TX(["I2C",0,[23,16,17,18,12,13]])
    bits[15]=True
    RX = MESSAGE()
    dict,json_rx = RX(bits)
    print(dict)
