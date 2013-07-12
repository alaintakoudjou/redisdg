#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import datetime
import time
from datetime import timedelta

import networkx as nx
import thread as th
import threading

import xml.dom.minidom

from MachineClass import MachineClass
from DataManager import DataManager

from BrokerClass import BrokerClass
from SchedulerClass import SchedulerClass
from WorkerClass import WorkerClass
from MonitorClass import MonitorClass
from CheckerClass import CheckerClass
from cloneKtimesGraph import cloneKtimesGraph

import logging
#import logging.handlers
import ConfigParser
from optparse import OptionParser, Option


logger = logging.getLogger("RedisDG")

class Parser(OptionParser):
  """
  Parse all arguments.
  """
  def __init__(self, usage=None, version=None):
    """
    Initialize all possible options.
    """
    OptionParser.__init__(self, usage=usage, version=version,
                          option_list=[
                            Option("-l", "--log_file",
                                   help="The path to the log file used by the script.",
                                   type=str),
                            Option('-i', "--pid_file",
                                   help="The path to the pid file used by the script.",
                                   default="",
                                   type=str),
                            Option("-t", "--install_directory",
                                   help="The path to use as RedisDG source directory.",
                                   default="",
                                   type=str),
                            Option("-s", "--server",
                                   help="The name or adresse of redis server.",
                                   default="redis.lipn.univ-paris13.fr",
                                   type=str),
                            Option("-r", "--directory",
                                   help="The path to use as Current work directory.",
                                   default="",
                                   type=str),
                            Option("-d", "--deamon_list",
                                   help="The list of process to start with redisDG. \n" +
                                   "Default is: broker,scheduler,worker,monitor,checker",
                                   default="broker,scheduler,worker,worker,worker,worker,monitor,checker",
                                   type=str),
                            Option("-v", "--verbose",
                                   default=False,
                                   action="store_true",
                                   help="Verbose output."),
                            Option("-c", "--console",
                                   default=False,
                                   action="store_true",
                                   help="Console output."),
                          ])

  def check_args(self):
    """
    Check arguments
    """
    (options, args) = self.parse_args()
    if not options.install_directory:
      options.install_directory = os.getcwd()
    return options

def setArguments():
  usage = "usage: %s [options]" % sys.argv[0]
  config = Parser(usage=usage).check_args()
  logger.setLevel(logging.INFO)
  if config.console:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(console_handler)
  if config.log_file:
    if not os.path.isdir(os.path.dirname(config.log_file)):
      # fallback to console only if directory for logs does not exists and
      # continue to run
      raise ValueError('Please create directory %r to store %r log file' % (
        os.path.dirname(config.log_file), config.log_file))
    else:
      file_handler = logging.FileHandler(config.log_file)
      file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
      logger.addHandler(file_handler)
      logger.info('Configured logging to file %r' % config.log_file)
  if config.pid_file:
    if not os.path.isdir(os.path.dirname(config.pid_file)):
      raise ValueError('Please create directory %r to store %r pid file' % (
        os.path.dirname(config.pid_file), config.pid_file))
    else:
      open(config.pid_file, 'w').write(str(os.getpid()))
  if config.directory:
    if not os.path.isdir(config.directory):
      raise ValueError('Please create directory %r to store local files' % (
        config.directory))
    else:
      os.chdir(config.directory)
  config.cwd = os.getcwd()

  return config


if __name__ == '__main__':

# Initilize the XML file which contains the application description


    config = setArguments()
 
    log_dir = os.path.join(config.cwd, 'log')
    pid_dir = os.path.join(config.cwd, 'pid')
    #log_dir = "log"
    #pid_dir = "pid"
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    if not os.path.exists(pid_dir):
        os.mkdir(pid_dir)

    logger = logging.getLogger("RedisDG-Main")
    logger.setLevel(logging.INFO)
    log_file = os.path.join(config.cwd, 'log', 'main.log')
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)
    logger.info('Configured logging to file %r' % log_file)
    #result_folder = os.path.join(os.path.abspath(config.directory), "result")
    if not os.path.exists("result"):
      os.mkdir("result")

    #file = 'table.xml'

    doc = xml.dom.minidom.parse("config.xml")

#
# Create an instance of a CodeServer (a data server for storing codes)
#

    d = DataManager(config.server, 'MD5')

#services
# Execute the python code (method 1)
#
#    d.ExecFileFromRedis('/tmp/e.sh');

#
# Execute the python code (method 2)
# We store the result on the DataServer
#

    d.server.flushdb()
    d.ListFileName()

#
# Load a the input file into the Redis DataServer
#

    dats = doc.getElementsByTagName('Data')
    for dat in dats:
        d.LoadFileIntoRedis(dat.childNodes[0].data, 'DATA')

      # d.LoadFileIntoRedis('input.txt','DATA');
#
# Load a python, bash source files into the Redis CodeServer
#

    cods = doc.getElementsByTagName('Code')
    for cod in cods:
        d.LoadFileIntoRedis(str(cod.childNodes[0].data), 'CODE')


      #
      # An example for executing a Workflow
      #

    G = nx.DiGraph()

      #
      # Code to extract graph nodes from XML file
      #

    nods = doc.getElementsByTagName('Node')
    for nod in nods:
        linput = []
        for i in nod.getElementsByTagName('Input'):
            linput.append(str(i.childNodes[0].nodeValue))
        loutput = []
        for j in nod.getElementsByTagName('Output'):
            loutput.append(j.childNodes[0].nodeValue)
        G.add_nodes_from(
            [int(nod.getAttribute('nodeID'))],
            Predecessors=[],
            CPU_MODEL=nod.getAttribute('CPU_MODEL'),
            SYST=nod.getAttribute('SYST'),
            AVAILABLE_RAM=nod.getAttribute('AVAILABLE_RAM'),
            AVAILABLE_DISK=nod.getAttribute('AVAILABLE_DISK'),
            FREE_UNTIL='',
            CODE=nod.getAttribute('CODE'),
            PARAMETERS=nod.getAttribute('PARAMETERS'),
            INPUT_FILE=linput,
            OUTPUT_FILE=loutput,
            )

      #
      # Code to extract graph edges from XML file
      #

    edgs = doc.getElementsByTagName('Edge')
    for edg in edgs:
        G.add_edges_from([(int(edg.getAttribute('source')),
                         int(edg.getAttribute('target')))])


      # extract Clone number from XML file
    
    NbClone = int(doc.getElementsByTagName('CloneNumber'
                  )[0].childNodes[0].data)

      #
      # We clone the Graph
      #

    GG = cloneKtimesGraph(G, NbClone)

    #print 'Nodes: ', GG.MyGraph.nodes()
    #print 'Edges: ', GG.MyGraph.edges()
  
    #for i in GG.MyGraph.nodes():
        #print i, '==>', GG.MyGraph.node[i]

    logger.info('Nodes: ' % GG.MyGraph.nodes())
    logger.info( 'Edges: ' % GG.MyGraph.edges())
    for i in GG.MyGraph.nodes():
        logger.info(str(i)+' ==> '+str(GG.MyGraph.node[i]))
    
    processDict = {'broker':False, 'scheduler':False, 'worker':False,
                   'monitor':False, 'checker':False}
    
    for key in config.deamon_list.split(','):
      if processDict.has_key(key):
        processDict[key] = True
    
    now = datetime.datetime(
      2063,
      8,
      4,
      12,
      30,
      45,
    )
    broker_pid = scheduler_pid = worker_pid = None
    worker_pid3 = worker_pid2 = worker_pid4 = None
    monitor_pid = None
    broker_pid = os.fork()
    if broker_pid == 0:
      #
      # As soon as a worker is completed, we restart a new worker
      # This is a simulation: we need to start workers in separate files
      #
      #Launch Broker Script
      if processDict['broker']:
        open("pid/broker.pid", 'w').write(str(os.getpid()))
        time.sleep(1.0)
        brocker = BrokerClass(
          GG,
          GG.MyGraph.order(),
          config.server,
          'Intel Core 2 Duo',
          1800,
          'Macos',
          4000,
          128,
          now,
          )
      else:
        exit(0)
    else:
      scheduler_pid = os.fork()
      if scheduler_pid == 0:
        if processDict['scheduler']:
          open("pid/scheduler.pid", 'w').write(str(os.getpid()))
          scheduler = SchedulerClass(
            config.server,
            'Intel Core 2 Duo',
            1800,
            'Macos',
            4000,
            128,
            now,
            )
        else:
          exit(0)
      else:
        worker_pid = os.fork()
        if worker_pid == 0:
          if processDict['worker']:
            open("pid/worker.pid", 'w').write(str(os.getpid()))
            worker1 = WorkerClass(
              GG,
              config.server,
              'Intel Core 2 Duo',
              1800,
              'Macos',
              4000,
              128,
              now,
              )
          else:
            exit(0)
        else:
          worker_pid2 = os.fork()
          if worker_pid2 == 0:
            if processDict['worker']:
              worker2 = WorkerClass(
                GG,
                config.server,
                'Intel Core 2 Duo',
                1800,
                'Macos',
                4000,
                128,
                now,
                )
            else:
              exit(0)
          else:
            worker_pid3 = os.fork()
            if worker_pid3 == 0:
              if processDict['worker']:
                worker3 = WorkerClass(
                  GG,
                  config.server,
                  'Intel Core 2 Duo',
                  1800,
                  'Macos',
                  4000,
                  128,
                  now,
                  )
              else:
                exit(0)
            else:
              worker_pid4 = os.fork()
              if worker_pid4 == 0:
                if processDict['worker']:
                  worker4 = WorkerClass(
                    GG,
                    config.server,
                    'Intel Core 2 Duo',
                    1800,
                    'Macos',
                    4000,
                    128,
                    now,
                    )
                else:
                  exit(0)
              else:
                monitor_pid = os.fork()
                if monitor_pid == 0:
                  if processDict['monitor']:
                    open("pid/monitor.pid", 'w').write(str(os.getpid()))
                    monitor = MonitorClass(
                      GG,
                      config.server,
                      'Intel Core 2 Duo',
                      1800,
                      'Macos',
                      4000,
                      128,
                      now,
                      )
                  else:
                    exit(0)
                else:
                  if processDict['checker']:
                    open("pid/checker.pid", 'w').write(str(os.getpid()))
                    checker = CheckerClass(
                      GG,
                      config.server,
                      'Intel Core 2 Duo',
                      1800,
                      'Macos',
                      4000,
                      128,
                      now,
                      )                  
    if monitor_pid:
      os.waitpid(monitor_pid, 0)
    if worker_pid:
      os.waitpid(worker_pid, 0)
    if worker_pid2:
      os.waitpid(worker_pid2, 0)
    if worker_pid3:
      os.waitpid(worker_pid3, 0)
    if worker_pid4:
      os.waitpid(worker_pid4, 0)
    if scheduler_pid:
      os.waitpid(scheduler_pid, 0)
    if broker_pid:
      os.waitpid(broker_pid, 0)


