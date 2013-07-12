#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import datetime
import time
from datetime import timedelta
import networkx as nx
import xml.dom.minidom
#from os import waitpid, P_NOWAIT, WIFEXITED, WEXITSTATUS, WIFSIGNALED, WTERMSIG
#from errno import ECHILD
import thread as th
import threading
import ast
import subprocess

from MachineClass import MachineClass
from DataManager import DataManager


import signal
import logging

###
# This class implements the 'workflow' engine
###

class BrokerClass(MachineClass):

    def __init__(
        self,
        batch,
        NbVertices,
        host,
        cpu,
        mhz,
        syst,
        ram,
        disk,
        free,
        ):

        # batch: task graph
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
        logger = logging.getLogger("RedisDG-Broker")
        logger.setLevel(logging.INFO)
        #log_file = os.path.join(config.cwd, 'log', 'broker.log')
        log_file = 'log/broker.log'
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
        # Code to initialise the publishing
        #
       
        em = d.server.pubsub()
        em.subscribe(Em)

        d.server.publish(WT, batch.MyGraph.order())
        mydico = {}


        pid1 = os.fork()
        if pid1 == 0 : 
            # batch.order():Return the number of nodes in the graph
            for i in batch.MyGraph.nodes():
                #print ('Broker publishes ' ,str(i) ,'***',  \
                #    batch.MyGraph.predecessors(i))
                logger.info('Broker publishes %s **** %s' %(str(i),batch.MyGraph.predecessors(i)))
                d.server.publish(WT, [str(i),
                             batch.MyGraph.predecessors(i)])

            #
            # Load a python source file into the Redis CodeServer
            #

                myfile = batch.MyGraph.node[i]['CODE']
                try:
                    mydico[myfile]
                except:
                    d.LoadFileIntoRedis(myfile, 'CODE')
                    #print 'The broker has downloaded the source file ', \
                        #myfile, ' into the Redis CodeServer'
                    logger.info('The broker has downloaded the source file %s into the Redis CodeServer' %myfile) 
                    mydico[myfile] = 'TRUE'
            print 'Broker is finishing'
            logger.info('Broker is finishing')
            #print 'Process parent (Broker) waits for the death of the child process which is listening to EMERGENCY',os.getpid()
        else :
            em = d.server.pubsub()
            em.subscribe(Em)
            for s in em.listen():
                pp = s['data']
                if pp == 'STOP' :
                    #print 'parent process (Broker) listen to EMERGENCY',os.getpid()
                    logger.info('parent process (Broker) listen to EMERGENCY %s' %os.getpid())
                        #time.sleep(3);
                    os.kill(pid1,signal.SIGKILL)
                    #print 'broker kills its child'
                    logger.info('broker kills its child')
                    #print 'process broker terminated'
                    logger.info('process broker terminated')
                    exit(12)
                    
                    break
    
