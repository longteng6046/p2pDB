#!/usr/bin/python

import threading
import socket

from sql.sqlChannel import *
from sql.sqlMsgListener import *
from pastry.peer import *

class Daemon:
    ##############################
    # Overall Daemon information
    ##############################p
    memId = None # member ID of this daemon

    ##############################
    # Pastry information
    ##############################
    pastryPeer = None # The pastry part ****************************************
    

    ##############################
    # SQL related information
    ##############################
    sqlBasePort = 12321 # to receive connection request
    sqlBaseBuf = None # to buffer connection request
    sqlPortStart = 13000 # port assigned to sql connection channels
    sqlPortEnd = 14000 #
    sqlPortCounter = sqlPortStart # current available port to be used for sql

    def __init__(self):
        print "A daemon started."

        # SQL part
        sqlPortCounter = self.sqlPortStart # current available port to be used for sql
        self.sqlMsgListener = SqlMsgListener(self.sqlBasePort, self.sqlBaseBuf, self)
        self.sqlMsgListener.setDaemon(True)
        self.sqlMsgListener.start()
        self.pastryPeer = Peer(None)
        
        # Operation Menu
        self.localOperation()
        

    def getCurrentSqlPort(self): # get the available sql port
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        flag = True
        while flag:
            flag = False
            self.sqlPortCounter += 1                        
            try:
                sock.bind(('localhost', self.sqlPortCounter))
            except:
                flag = True
        sock.close()
        return self.sqlPortCounter

    # send hostName a request, establish a Channel on the given
    #   port, and return that channel
    def sqlConnect(self, objHost):
        channel = SqlChannel(objHost, self.sqlBasePort, self.getCurrentSqlPort(), "inConnectFunc", True)
        channel.setDaemon(True)
        channel.start()
        channel.connect()
        return channel

    def localOperation(self):
        while (True):
            print '''Please select operation:
            \t'c') To establish a connection with another daemon.
            \t'peer') To operate pastry peer.
            \t'q') To quit.'''

            option = raw_input("Your command: ")

            if option == 'q':
                return

            elif option == 'peer':
                self.pastryPeer.localOperation()
            elif option == 'c':
                hostname = raw_input("Which host do you want to connect to?\n")
                channel = self.sqlConnect(hostname)
                while True:
                    content = raw_input("Input sth to send:\n")
                    channel.send(content)
                    op = raw_input("continue?")
                    if op != 'y' and op != 'yes':
                        break

                print "the channel is terminated."
##############################
# Main
##############################

Daemon()
