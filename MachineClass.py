#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import datetime
import time
from datetime import timedelta
import networkx as nx
import random
import ast
import subprocess
import stat


L_CPU = ['Intel Xeon', 'Intel Core 2 Duo', 'Intel Atom', 'Nvidia Tegra'
         , 'Qualcomm Snapdragon']

L_OS = ['Linux', 'Macos', 'Android', 'Windows']

class EngineClass:

    def __init__(self, MyGraph, NodeList):

        #
        # MyGraph: the workflow (a networkx graph instance)
        # NodeList: the list of nodes of the MyGraph graph
        #

        r = redis.Redis(host=hostname, port=6379, db=0)

        # Go. We subscribe to FinishedTasks channel

        rr = r.pubsub()
        rr.subscribe('FinishedTask')

        #
        # We assume that we manage SP graphs: we have only one initial state
        # and all the other states go to WaitingStates
        #

        r.publish('TasksToDo', list(NodeList[0]))
        r.publish('WaitingStates', NodeList[:1])
        while True:

           # bla bla

            break


class MachineClass(EngineClass):

    # ##
    # The abstract data type for a computer
    # ##

    CPU_MODEL = ''
    MHZ = 0
    OS = ''
    AVAILABLE_RAM = 0
    AVAILABLE_DISK = 0
    FREE_UNTIL = datetime.datetime(  # See Timer class for controling the work duration
        2063,
        8,
        4,
        12,
        30,
        45,
        )

    def __init__(
        self,
        cpu,
        mhz,
        os,
        ram,
        disk,
        mydate,
        ):
        if type(cpu) == str and cpu in L_CPU:
            self.CPU_MODEL = cpu
        else:
            print L_CPU
            sys.exit('CPU not in the list above or wrong type object')
        if type(mhz) == int and mhz < 5000:
            self.MHZ = mhz
        else:
            sys.exit('MHZ has a wrong type object')
        if type(cpu) == str and cpu in L_CPU:
            self.OS = os
        else:
            print L_OS
            sys.exit('Operating system not in the list above or wrong type object'
                     )
        if type(ram) == int:
            self.AVAILABLE_RAM = ram  # in kilo bytes
        else:
            sys.exit('Ram (KB): wrong type object. Need an integer')
        if type(disk) == int:
            self.AVAILABLE_DISK = disk  # in giga bytes
        else:
            sys.exit('Disk (GB): wrong type object. Need an integer')
        if type(mydate) == timedelta or type(mydate) \
            == datetime.datetime:
            self.FREE_UNTIL = mydate
        else:
            sys.exit('Free_until: wrong type object. Need a datetime or datetime.timedelta object'
                     )

    def startW(self, hostname):
        print 'start'
        r = redis.Redis(host=hostname, port=6379, db=0)

        # publication of the information 'I'am ready for working' on channel AskWork
        # and with my properties

        r.publish('ASKWORK', self.CPU_MODEL + '|' + str(self.MHZ) + '|'
                  + self.OS + '|' + str(self.AVAILABLE_RAM) + '|'
                  + str(self.AVAILABLE_DISK) + '|'
                  + str(self.FREE_UNTIL))

        # Go. We subscribe to GIVEWORK channel

        rr = r.pubsub()
        rr.subscribe('GIVEWORK')
        for msg in rr.listen():
            print msg
            if msg['data'] == 'STOPWORK':
                break
        self.stop()

    def startC(self, G):
        t = EngineClass(G, G.nodes())

    def stop(self):
        print 'stop'
        sys.exit('Worker exits...')

