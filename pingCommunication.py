#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import socket
import subprocess

from MachineClass import MachineClass
from DataManager import DataManager


class pingCommunication:

    # ipToPing: address to ping
    # pQ: number of ping messages sent to ipToPing
    # pw: seconds to wait between ping messages

    def __init__(
        self,
        ipToPing,
        pQ,
        pW,
        ):
        self.ipToPing = str(ipToPing)
        self.pingQuantity = str(pQ)  # number of ping
        self.pingWait = str(pW)  # wait pW seconds between pings

    def pingProcess(self):
        pingTest = 'ping -c ' + self.pingQuantity + ' -i ' \
            + self.pingWait + ' ' + self.ipToPing

        # print pingTest

        process = subprocess.Popen(pingTest, shell=True,
                                   stdout=subprocess.PIPE)
        process.wait()
        returnCodeTotal = process.returncode
        if returnCodeTotal == 0:
            print 'Ping sucessfull for all packets'
        else:
            print 'Failure with ping'
