#!/usr/bin/python
# -*- coding: utf-8 -*-
import zlib
import sys
import os
import shlex
import MyRedis2410 as redis
import random
import ast
import subprocess
import stat
import hashlib

import signal
import logging

class ServerClass:

    # ##
    # There are 3 possibilities for servers: the main server for the protocol itself (CoreServer),
    # the data server storing input/output data for the applications and
    # CodeServer which is the name of the server for retreiving codes.
    # Note also that the Redis databases are different for each server.
    # ##

    Server = None
    typeintegrety = None

    def __init__(
        self,
        Core='localhost',
        tip='MD5',
        ):
        self.Server = redis.Redis(host=Core, port=6379, db=0)

        # Now, the integrety of files

        if ['MD5'].__contains__(tip):
            self.typeintegrity = tip
        else:
            raise 'Type Error for the file integrity (ServerClass)'


class DataManager(ServerClass):

    server = None

    def __init__(
        self,
        Core='localhost',
        tip='MD5',
        ):

        self.server = ServerClass(Core, tip).Server


    # ##
    # List all the file name stored in the DataServer
    # ##

    def ListFileName(self):

        #self.config = config
        logger = logging.getLogger("RedisDG-Main")
        logger.setLevel(logging.INFO)
        #log_file = os.path.join(config.cwd, 'log', 'broker.log')
        log_file = 'log/main.log'
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        logger.addHandler(file_handler)
        logger.info('Configured logging to file %r' % log_file)



        #print 'File(s) on DataServer: ', self.server.keys('*')
        #print 'File(s) on CodeServer: ', self.server.keys('*')
        logger.info('File(s) on DataServer: ' %self.server.keys('*'))
        logger.info('File(s) on CodeServer: ' %self.server.keys('*'))

    # ##
    # Load a file into a Redis Server
    # ##

    def LoadFileIntoRedis(self, filename, mytype):
        f = open(filename, 'r')
        mystr = f.read()
        res = zlib.compress(mystr)
        if mytype == 'CODE':
            self.server.append(filename, res)
        else:
            self.server.append(filename, res)
        f.close()

    # ##
    # Execute locally a file stored in the CodeServer
    # The code do not require any parameter and the result
    # is not stored
    # ##

    def ExecFileFromRedis(self, filename):
        mystr = self.server.get(filename)
        if mystr != None:
            res = zlib.decompress(mystr)
            f = open(filename, 'r+w+x')
            f.write(res)
            f.close()
            p = subprocess.Popen(args=[], executable=filename,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, shell=True)
            outputlines = p.stdout.readlines()
            p.wait()
            #print outputlines
            logger.info(outputlines)
        else:
            raise 'error trying to execute ', filename, \
                ' in ExecFileFromRedis'

    # ##
    # Execute locally a command that specify arguments on the command line
    # The first name of the command_line is the file name
    # The output_file parameter is the name of the outfile: we store the
    # result in the Redis server.
    # Command line format: 'python essai.py 'bonjour' 'a vous' 'tous'
    # ##

    def ExecCommandLineFromRedis(self, command_line, output_file):
        args = shlex.split(command_line)
        filename = args[1]  # args[1] = 'python', we assume that args[0]=/bin/bash or perl or python...
        mystr = self.server.get(filename)
        if mystr != None:
            res = zlib.decompress(mystr)

            # we dump the code on the local disk

            filout = open(filename, 'w')
            filout.write(res)
            filout.close()

            # we start the execution

            out = subprocess.check_output(args)

            #            sts = os.waitpid(p.pid, 0)[1]
            # we compress and we store the result into Redis

            res = zlib.compress(out)
            self.server.append(output_file, res)

            # we remove the Python source file

            os.remove(filename)
        else:
            raise 'error trying to execute ', filename, \
                ' in ExecCommandLineFromRedis'

    # ##
    # Execute locally a code.
    # First we download the code, then the input files
    # Second we execute the code
    # Third we store the output file into the Redis server
    # At least we remove the files to clean the partition
    # ##

    def ExecCodeFromRedis(
        self,
        language,
        code_name,
        list_input_files,
        list_output_files,
        ):

        # We download source/exec file

        #print 'ExecCodeFromRedis: ', code_name, ' ', list_input_files, \
        #    ' ', list_output_files
        mystr = self.server.get(code_name)
        if mystr != None:
            res = zlib.decompress(mystr)
            info = random.randint(1, 10000)

            # we make unique the file name

            filout = open('/tmp/' + str(info) + code_name, 'w')
            filout.write(res)
            filout.close()
            os.chmod('/tmp/' + str(info) + code_name, stat.S_IRWXU)

        # we download the input files

        for filename in list_input_files:
            mystr = self.server.get(filename)
            if mystr != None:
                res = zlib.decompress(mystr)
                filout = open('/tmp/' + filename, 'w')
                filout.write(res)
                filout.close()
            else:
                self.ListFileName()
                print 'error trying to execute ', filename, \
                    ' in ExecCodeFromRedis'
                raise 'error trying to execute in ExecCodeFromRedis'

        # we start the execution

        input_files = ''
        for f in list_input_files:
            input_files = input_files + ' ' + '/tmp/' + f
        output_files = ''
        for f in list_output_files:
            output_files = output_files + ' ' + '/tmp/' + f
        Myargs = language + ' ' + '/tmp/' + str(info) + code_name + ' ' \
            + input_files + ' ' + output_files
        #print 'Args: ', Myargs
       
        
        p = subprocess.Popen(['/tmp/' + str(info) + code_name + ' '
                             + input_files + ' ' + output_files],
                             shell=True, executable='/bin/bash')
        p.wait()

        # We store the output files into the Redis DataServer

        for filename in list_output_files:
            f = open('/tmp/' + filename, 'r')
            mystr = f.read()
            res = zlib.compress(mystr)
            self.server.append(filename, res)
            f.close()
        #print 'File located on Data and Code servers'
        self.ListFileName()


        # we remove the Python source file, the input and output files
        # os.remove('/tmp/'+str(info)+code_name)
        #
        # Attention a ce remove
        #
        # for f in list_input_files:
            # os.remove('/tmp/'+f)
        # for f in list_output_files:
            # os.remove('/tmp/'+f)
