#!/usr/bin/python
# -*- coding: utf-8 -*-
import zlib
import sys
import os
import shlex
import datetime
import time
from datetime import timedelta
import networkx as nx
import thread as th
import threading
import ast
import subprocess
import hashlib
import xml.dom.minidom
#from os import waitpid, P_NOWAIT, WIFEXITED, WEXITSTATUS, WIFSIGNALED, WTERMSIG
#from errno import ECHILD

from MachineClass import MachineClass
from DataManager import DataManager

import signal
import logging

###
# Class Checker inherits from Machine
# This class implements the behavior of a Checker
###

class CheckerClass(MachineClass):

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
        logger = logging.getLogger("RedisDG-Checker")
        logger.setLevel(logging.INFO)
        #log_file = os.path.join(config.cwd, 'log', 'checker.log')
        log_file = 'log/checker.log'
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
        rr.subscribe(TTV)

        em = d.server.pubsub()
        em.subscribe(Em)

        # extract Clone number from XML file
    
        NbClone = int(doc.getElementsByTagName('CloneNumber'
                  )[0].childNodes[0].data)

        #
        # initialisation of the dictionnary of tasks received by the checker
        #
        pid1 = os.fork()
        if pid1 == 0 :
            dico = {}
            for msg in rr.listen():
                c = str(ast.literal_eval(msg['data']))
                #print 'Cheker receives the task: ', c
                logger.info ('Cheker receives the task: %s ' %c)

            # The task 1.0 is not checked by the checker : it is treated otherwise

                if c != '1.0':

                # Calculate the MD5 of the outputFile of the task c

                    filename = batch.MyGraph.node[c]['OUTPUT_FILE'][0]
                    output = zlib.decompress(d.server.get(filename))
                    output_file = os.path.join("result", filename)
                    with open(output_file, 'w') as f:
                        f.write(output)
                    try:
                        cmd5 = hashlib.md5(output).hexdigest()
                    except :
                        print 'Error: Enable to calculate MD5'
                        logger.info('Error: Enable to calculate MD5')
                    if dico != {}:
                        k = 0  # Counter which increase the number of received duplication.
                        dicoCheck = {}  # Dictionnary of duplicata of a task.
                        dicoCheck[c] = cmd5  # the dictionnary is initialized by the first duplicata received.

                        for key in dico:  # Check the existance of a duplicata of a received task
                            if c[0] == key[0] and k < NbClone:  # in the dictionnary of tasks (dico)
                                dicoCheck[key] = dico[key]  # and save all the duplicata of the same task in another dictionnary (dicoCheck)
                                k = k + 1

                        dico[c] = cmd5

                        if k == NbClone:
                            for i in range(len(dicoCheck.values()) - 1):  # We compare list elements which are
                                boool = False  # (MD5) of dictionnary of duplicata (dicoCheck)
                                if dicoCheck.values()[i] \
                                        == dicoCheck.values()[i + 1]:

                                # print '///////////////////////////////', dicoCheck.values()[i]

                                    boool = True
                            if boool == True:
                                for key in dicoCheck:
                                    d.server.publish(FT, key)
                                    del dico[key]
                                #print 'The result of tasks ', \
                                #    dicoCheck.keys(), \
                                #    ' is checked.... It is OK :-)'
                                logger.info('The result of tasks %s is checked.... It is OK :-)' %dicoCheck.keys())
                            else:
                                for key in dicoCheck:
                                    d.server.publish(TTD, key)
                                    del dico[key]
                                #print 'The result of tasks ', \
                                #    dicoCheck.keys(), \
                                #    ' is checked.... It is not OK :-('
                                logger.info('The result of tasks %s is checked.... It is not OK :-(' %dicoCheck.keys())
                    else:
                        dico[c] = cmd5
                else:
                    d.server.publish(FT, c)
                    #print 'Checker is publishing task', c, \
                        #'on FinishedTasks channel'
                    logger.info('Checker is publishing task %s on FinishedTasks channel'%c)
        else:
                em = d.server.pubsub()
                em.subscribe(Em)
                for s in em.listen():
                    pp = s['data']
                    if pp == 'STOP' :
                        #print 'Child process (Checker) listen to EMERGENCY',os.getpid()
                        logger.info ('Child process (Checker) listen to EMERGENCY %s'%os.getpid())
                        #time.sleep(3);
                        os.kill(pid1,signal.SIGKILL)
                        #print 'checker kills its child'
                        logger.info('checker kills its child')
                        #print 'process checker terminated'
                        logger.info('process checker terminated')
                        exit(1)

