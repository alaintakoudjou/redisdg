#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import networkx as nx
import subprocess

###
# CloneKtimesGraph Class
# This class duplicates a given graph K times
###

class cloneKtimesGraph:

    MyGraph = None

    def __init__(self, G, K):

        # nb of backup copies: K
        # original graph: G

        Gclone = nx.DiGraph()

        # Add nodes of the new graph

        for i in range(G.__len__()):

            # Gclone.add_node(str(i+1)+".0")

            d1 = [str(i + 1) + '.0']
            Gclone.add_nodes_from(
                d1,
                Predecessors=G.node[i + 1]['Predecessors'],
                CPU_MODEL=G.node[i + 1]['CPU_MODEL'],
                SYST=G.node[i + 1]['SYST'],
                AVAILABLE_RAM=G.node[i + 1]['AVAILABLE_RAM'],
                AVAILABLE_DISK=G.node[i + 1]['AVAILABLE_DISK'],
                FREE_UNTIL=G.node[i + 1]['FREE_UNTIL'],
                CODE=G.node[i + 1]['CODE'],
                PARAMETERS=G.node[i + 1]['PARAMETERS'],
                INPUT_FILE=G.node[i + 1]['INPUT_FILE'],
                OUTPUT_FILE=G.node[i + 1]['OUTPUT_FILE'],
                )
            for j in range(K):
                if i != 0:

                     # Gclone.add_node(str(i+1)+'.'+str(j+1))

                    d1 = [str(i + 1) + '.' + str(j + 1)]
                    out__file = G.node[i + 1]['OUTPUT_FILE']
                    Myoutfile = []
                    for vv in out__file:
                        Myoutfile.append(vv + '.' + str(j + 1))
                    Gclone.add_nodes_from(
                        d1,
                        Predecessors=G.node[i + 1]['Predecessors'],
                        CPU_MODEL=G.node[i + 1]['CPU_MODEL'],
                        SYST=G.node[i + 1]['SYST'],
                        AVAILABLE_RAM=G.node[i + 1]['AVAILABLE_RAM'],
                        AVAILABLE_DISK=G.node[i + 1]['AVAILABLE_DISK'],
                        FREE_UNTIL=G.node[i + 1]['FREE_UNTIL'],
                        CODE=G.node[i + 1]['CODE'],
                        PARAMETERS=G.node[i + 1]['PARAMETERS'],
                        INPUT_FILE=G.node[i + 1]['INPUT_FILE'],
                        OUTPUT_FILE=Myoutfile,
                        )

        # Add edges of the new graph
        # we duplicate edges of G

        for j in range(K + 1):
            for i in G.edges():
                if i[0] == 1:
                    src = str(i[0]) + '.0'
                else:
                    src = str(i[0]) + '.' + str(j)
                dest = str(i[1]) + '.' + str(j)
                Gclone.add_edges_from([(src, dest)])
        self.MyGraph = Gclone

