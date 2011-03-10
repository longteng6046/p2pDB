from listener import Listener;

import sys
import threading
import socket
import random

class Communicator:
    messageQueue = None 
    def __init__(self, messageQueue):
        print "A communicator is created!"
        self.messageQueue = messageQueue
        self.mylistener = Listener("", 10086, self.messageQueue)
        self.mylistener.setDaemon(True)
        self.mylistener.start()
        # self.operation()
        
    def send(self, host, port, content):
        print "sending ..."

        # socket setting 
        buf = 1024
        addr = (host, port)

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(content, addr)
        sock.close()
        
    def rcv(self, myPId):
        msgList = []
        newMsgQueue = []
        for item in self.messageQueue:
            msgList.append(item)
        self.messageQueue = newMsgQueue
        return msgList

    def prtMsgQueue(self):
        for item in self.messageQueue:
            print item

    def operation(self):
        while True:
            print '''Please select operation:
            \t'send') send a message to a host;
            \t'prt') print all received messages
            \t'q') logout peer.'''
            option = raw_input("Your command: ")
            
            if option == 'send':
                host = raw_input("hostname: ")
                content = raw_input("content that you want to send: ")
                self.send(host, 10086, content)
            elif option == 'prt':
                print self.messageQueue
            elif option == 'q':
                break
            else:
                print "Please choose an option:"
                continue