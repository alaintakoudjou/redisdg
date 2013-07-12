#!/usr/bin/python
# -*- coding: utf-8 -*-
import zlib
import sys
import os
import socket
import MyRedis2410 as redis
import networkx as nx
import thread as th
import threading
import random
import ast
import subprocess
import xml.dom.minidom
from os import waitpid, P_NOWAIT, WIFEXITED, WEXITSTATUS, WIFSIGNALED, WTERMSIG
from errno import ECHILD

from MachineClass import MachineClass
from DataManager import DataManager

import signal
import logging

###
# Class Worker inherits from Machine
# This class implements the behavior of a worker
###

class WorkerClass(MachineClass):

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
        
        import time
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
        logger = logging.getLogger("RedisDG-Worker")
        logger.setLevel(logging.INFO)
        #log_file = os.path.join(config.cwd, 'log', 'worker.log')
        log_file = 'log/worker.log'
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
        rr.subscribe(TTD)
        ss = d.server.pubsub()
        ss.subscribe(SV)

        em = d.server.pubsub()
        em.subscribe(Em)

        random.seed()
        info = random.randint(1, 1000000)
        pid1 = os.fork()
        if pid1 == 0:
            for msg in rr.listen():
                #print 'worker receives the task: ', \
                    #msg['data']
                logger.info ('worker receives the task: %s' %msg['data'])
                c = str(ast.literal_eval(msg['data']))

                # The worker publish that it is interested by a flag which is a random int
                # the worker waits to be selected by a message which contains the random int
                # if the random int received is equal to the random int emitted then do the code bellow else do nothing

                d.server.publish(VW, (str(info), c))
                #print 'Worker publishes: i am volunteers +++++', info
                logger.info ('Worker publishes: i am volunteers  %s' %info)
        else:
             pid2 = os.fork()
             if pid2 == 0 : 
                     for msg_return in ss.listen():
                         cc = ast.literal_eval(msg_return['data'])
                         #print 'worker with info:', info, \
                             #'listen to the message', cc, \
                             #'on the channel SelectVolunteers'
                         logger.info('worker with info: %s listen to the message%s on the channel SelectVolunteers' %(info,cc) )

                # break;# au premier on sort

                         if cc[0] == str(info):
                             print 'Worker ',info, ' is executing task ', \
                             cc[1]
                             logger.info('Worker %s is executing task %s' %(info,cc[1]) )
                             process_id = os.getppid()

                             my_ip = socket.gethostbyname(socket.gethostname())

                             d.server.publish(TIP, [cc[1], my_ip, process_id])

                             #print 'Worker ', info, ' is publishing task ', \
                             #cc[1], 'on TasksInProgress channel'
                             logger.info('Worker %s is publishing task %s on TasksInProgress channel' %(info,cc[1]))

                    #

                             myfile = batch.MyGraph.node[cc[1]]['CODE']
                             if myfile.endswith('.py'):
                                 language = 'python'
                             else:
                                 language = '/bin/bash '
                             #print 'I start the execution of ', myfile, \
                             #' from code on server'
                             logger.info('I start the execution of %s from code on server' %myfile )
                             d.ExecCodeFromRedis(language, myfile,
                                             batch.MyGraph.node[cc[1]]['INPUT_FILE'],
                                             batch.MyGraph.node[cc[1]]['OUTPUT_FILE'])

                    #

                             #print 'Worker ', info, ' is finishing task ', \
                             #cc[1]
                             logger.info('Worker %s is finishing task %s' %(info,cc[1]))

                    #

                             d.server.publish(TTV, cc[1])
                             #print 'Worker ', info, ' is publishing task ', \
                             #cc[1], 'on TasksToVerify channel'
                             logger.info('Worker %s is publishing task %s on TasksToVerify channel' %(info,cc[1]))
             else :
                     em = d.server.pubsub()
                     em.subscribe(Em)
                     for s in em.listen():
                         pp = s['data']
                         if pp == 'STOP' :
                             #print 'parent process (worker) listen to EMERGENCY',os.getpid()
                             logger.info('parent process (worker) listen to EMERGENCY %s' %os.getpid())
                        #time.sleep(3);
                             import signal
                             os.kill(pid1,signal.SIGKILL)
                             os.kill(pid2,signal.SIGKILL)
                             #print 'worker kills its child'
                             logger.info('worker kills its child')
                             #print 'process worker terminated'
                             logger.info('process worker terminated')
                             
                             exit(1)


