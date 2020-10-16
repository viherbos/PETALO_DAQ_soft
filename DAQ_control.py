#!/home/viherbos/anaconda2/bin/python

#cd h:\Compartido\PETALO\petalo_daq_soft_master
import socket as sk
import sys
import json
from threading import Thread, Event
from queue import Queue, Empty
import readline as readline
import atexit
import os
import time
from DAQ_control_Lib.py_comm_lib import SCK_TXRX
from DAQ_control_Lib.py_comm_lib import MESSAGE
from DAQ_control_Lib.py_comm_lib import LOGGER
from DAQ_control_Lib.py_comm_lib import v_print
from DAQ_control_Lib.config import DATA
import ast




if __name__ == "__main__":

    #os.system('clear')

    # History file
    histfile = ".petalo_hist"
    try:
        readline.read_history_file(histfile)
        # default history len is -1 (infinite), which may grow unruly
        readline.set_history_length(1000)
    except IOError:
        pass
    atexit.register(readline.write_history_file, histfile)
    del os, histfile


    # Create queues
    cfg_data = DATA(read=True)
    tx_queue = Queue()
    txlog_queue = Queue()
    rx_queue = Queue()
    stopper = Event()
    M = MESSAGE()


    with open("petalo_log.txt", "w") as pfile:

        thread_LOG1  = LOGGER(cfg_data,rx_queue,stopper,0,pfile)
        thread_LOG2  = LOGGER(cfg_data,txlog_queue,stopper,1,pfile)
        thread_TXRX  = SCK_TXRX(cfg_data,tx_queue,rx_queue,stopper)


        thread_TXRX.start()
        thread_LOG1.start()
        thread_LOG2.start()

        print("\nCommand Format  : [C_id,L1_id,[args1,arg2,...]]")
        print("Batch execution : run batch_name.txt \n")

        while not stopper.is_set():
            try:
                #time.sleep(0.5)
                command = input("\n>>")
                while command=="":
                    command = command = input("\n>>")
            except KeyboardInterrupt:
                print ("Keyboard Interrupt")
                break

            except:
                pass
                print("\n Bad command format [1] \n"),
                sys.stdout.flush()

            else:
                if (command[0:3]=="run"):
                    try:
                        with open(command[4:],"r") as batch_file:
                            batch_list = batch_file.readlines()
                        for command in batch_list:
                            if command=="\n":
                                continue
                            else:
                                #print(command)
                                json_command = json.loads(command)
                                message_bits = M(json_command)
                                tx_queue.put(message_bits)
                                txlog_queue.put(message_bits)
                                print("MENSAJE ENVIADO")
                    except:
                        print("\n Can't open file \n")

                else:
                     message_bits = M(ast.literal_eval(command))
                     if (message_bits != -1):
                         tx_queue.put(message_bits)
                         txlog_queue.put(message_bits)
                    # try:
                    #     #print(ast.literal_eval(command))
                    #     message_bits = M(ast.literal_eval(command))
                    # except:
                    #     print("\n Bad command format [2] \n"),
                    #     sys.stdout.flush()
                    # else:
                    #     if (message_bits != -1):
                    #         tx_queue.put(message_bits)
                    #         txlog_queue.put(message_bits)


        stopper.set()
        thread_LOG1.join()
        thread_LOG2.join()
        thread_TXRX.join()
