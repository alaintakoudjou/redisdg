#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import shlex
import socket
import datetime
import time
from datetime import timedelta
import MyRedis2410 as redis
import networkx as nx
import thread as th
import threading
import random
import ast
import subprocess
import stat
import xml.dom.minidom

#from os import waitpid, P_NOWAIT, WIFEXITED, WEXITSTATUS, WIFSIGNALED, WTERMSIG
#from errno import ECHILD

from MachineClass import MachineClass
from DataManager import DataManager
from pingCommunication import pingCommunication

import logging


###
# Class Monitor inherits from Machine
# This class implements the behavior of a machine
###

lock_mmap = threading.Lock()


class MonitorClass(MachineClass):
    
    #def Myhandler(self, signum, frame):
    #    print 'Signal handler called with signal', signum, ' (SIGCHLD)'
    #    os.wait()

    def __init__(
        self,
        batch,
        host,
        cpu,
        mhz,
        syst,
        ram,
        disk,
        free,
        ):

        #
        # host: server Redis where we publish topics
        # meaning of other parameters is clear
        #

        m = MachineClass(
            cpu,
            mhz,
            syst,
            ram,
            disk,
            free,
            )

         #self.config = config
        logger = logging.getLogger("RedisDG-Monitor")
        logger.setLevel(logging.INFO)
        #log_file = os.path.join(config.cwd, 'log', 'monitor.log')
        log_file = 'log/monitor.log'
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        logger.addHandler(file_handler)
        logger.info('Configured logging to file %r' % log_file)

        # Initilize the XML file which contains the application description

        file = 'config.xml'

        doc = xml.dom.minidom.parse(file)        

        #
        # DataManager instance
        #

        d = DataManager(host, 'MD5')

        #
        # Number of tasks to monitor + Shared memory initialisation
        #

        try :
            nttm = batch.MyGraph.order()
        except :
            print 'Error : Enable to get graph order'
            logger.info('Error : Enable to get graph order')
        dico_mmap = {}
        i = 0
        for n in batch.MyGraph.nodes():
            dico_mmap[n] = i
            i = i + 1

        import mmap
        import os
        import signal
        import struct
        import ctypes

        # Create new empty file to back memory map on disk

        fd = os.open('/tmp/mmaptest', os.O_CREAT | os.O_TRUNC
                     | os.O_RDWR)

        # Zero out the file to insure it's the right size

        assert os.write(fd, '\x00' * mmap.PAGESIZE) == mmap.PAGESIZE

        # Create the mmap instance with the following params:
        # fd: File descriptor which backs the mapping or -1 for anonymous mapping
        # length: Must in multiples of PAGESIZE (usually 4 KB)
        # flags: MAP_SHARED means other processes can share this mmap
        # prot: PROT_WRITE means this process can write to this mmap

        buf = mmap.mmap(fd, mmap.PAGESIZE, mmap.MAP_SHARED,
                        mmap.PROT_WRITE)
        j = 0
        jj = ctypes.c_int.from_buffer(buf, 0)
        offset = 0
        while j < nttm:

            # Now create an int in the memory mapping

            i = ctypes.c_int.from_buffer(buf, offset)

            # Set a value

            i.value = j

            # Before we create a new value, we need to find the offset of the next free
            # memory address within the mmap

            offset = (j + 1) * struct.calcsize(i._type_)
            if j + 1 != nttm:

                # The offset should be uninitialized ('\x00')

                assert buf[offset] == '\x00'

            # increase the offset

            j = j + 1

        #
        # After the code above, we have a mapping: offset -> process id, in fact
        # node_id -> process id
        # Example: is offset is 4, this means that task 4 (corresponding to the node_id
        # '1.2' in the graph for instance) will be monitoring by the process id stored in
        # buf[offset*sizeof(int)]

        # Extract Data from XML file : channels

        FT = doc.getElementsByTagName('FinishedTasks')[0].childNodes[0].data
        Em = doc.getElementsByTagName('Emergency')[0].childNodes[0].data
        WT = doc.getElementsByTagName('WaitingTasks')[0].childNodes[0].data
        TTD = doc.getElementsByTagName('TasksToDo')[0].childNodes[0].data
        TIP = doc.getElementsByTagName('TasksInProgress'
                                   )[0].childNodes[0].data
        TTV = doc.getElementsByTagName('TasksToVerify'
                                   )[0].childNodes[0].data
        SV = doc.getElementsByTagName('SelectVolunteers'
                                  )[0].childNodes[0].data
        VW = doc.getElementsByTagName('VolunteerWorkers'
                                  )[0].childNodes[0].data

        #
        # Code to initialise the subscription
        #

        rr = d.server.pubsub()
        rr.subscribe(TIP)
        tv = d.server.pubsub()
        tv.subscribe(TTV)

        #
        # We create one process for incoming requests for monitoring
        # and one process for stopping the monitoring
        #

        # Set the signal handler when signal SIGQUIT is sent to kill a process

        #signal.signal(signal.SIGCHLD, self.Myhandler)

        pid1 = os.fork()
        if pid1 == 0:
            for msg in rr.listen():
                #print 'Monitor receives the task: ', msg['data']
                logger.info('Monitor receives the task: %s ' % msg['data'])
                my_ip = ast.literal_eval(msg['data'])[1]

                my_task = ast.literal_eval(msg['data'])[0]

                pid11 = os.fork()
                if pid11 == 0:

                    #
                    # We store the process id that monitors task my_task
                    #

                    lock_mmap.acquire()

                    # print '!!!!!!!!!!!', my_task
                    # print '***********', dico_mmap[my_task]
                    # offset1 = dico_mmap[my_task]*struct.calcsize(jj._type_)

                    offset1 = dico_mmap[my_task] * 4
                    i = ctypes.c_int.from_buffer(buf, offset1)
                    i.value = os.getpid()
                    #print 'PID to monitor :', i.value
                    logger.info('PID to monitor : %s ' %i.value)
                    comunicate = pingCommunication(my_ip, 6, 2)
                    lock_mmap.release()

                    #
                    # We start the monitoring until we kill it
                    #

                    while True:
                        comunicate.pingProcess()
                #else:

                    # on cree des processus zombie bien que le pere
                    # soit sur un waitpid non bloquant pour recevoir l'info que
                    # le processus a ete tue et ensuite pour liberer
                    # toutes les ressources detenues par le processus
                    # en particulier le PID
                    # Le programme tourne, mais les zombies restent !

                    #(p, status) = os.waitpid(pid1, os.P_NOWAIT)
                    #(p, status) = os.wait()
        else:
            pid2 = os.fork()
            if pid2 == 0 :
                for gsm in tv.listen():

                    #print 'Monitor receives the request for stopping monitoring ', gsm['data']
                    logger.info('Monitor receives the request for stopping monitoring %s ' %gsm['data'])
                    my_task = str(ast.literal_eval(gsm['data']))

                    #
                    # We kill the monitoring of task my_task
                    #

                    lock_mmap.acquire()

                    j = dico_mmap[my_task]
                    (pid_i, ) = struct.unpack('i', buf[j * 4:j * 4 + 4])
                    #print 'The Monitor kills the supervision process', \
                        #pid_i, ' type: ', type(pid_i)
                    logger.info('The Monitor kills the supervision process %s type: %s' %(pid_i,type(pid_i)))

                    os.kill(pid_i, signal.SIGKILL)
                    lock_mmap.release()
            else:
                    em = d.server.pubsub()
                    em.subscribe(Em)
                    for s in em.listen():
                        pp = s['data']
                        if pp == 'STOP' :
                            #print '++++++++++++parent process (monitor) listen to EMERGENCY',os.getpid()
                            logger.info('++++++++++++parent process (monitor) listen to EMERGENCY %s' %os.getpid())
                            #time.sleep(3);
                            import signal
                            os.kill(pid1,signal.SIGKILL)
                            os.kill(pid2,signal.SIGKILL)
                            #print 'monitor kills its child'
                            logger.info('monitor kills its child')
                            #print 'process monitor terminated'
                            logger.info('process monitor terminated')
                            exit(1)
