from listener import Listener;

import sys
import threading
import socket
import random

class Communicator:
    messageQueue = None 
    def __init__(self, peer):
        # print "A communicator is created!"
        self.messageQueue = peer.messageQueue
        self.peer = peer
        self.mylistener = Listener(peer)
        self.mylistener.setDaemon(True)
        self.mylistener.start()
        # self.operation()
        
    def send(self, host, sendPort, content):
        # print "sending ..."

        # socket setting 
        buf = 1024 * 1024
        addr = (host, sendPort)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(addr)
        except Exception:
            print "Connection lost with: ", addr
            return False
        sock.send(content)
        sock.close()
        return True
        # sock.sendto(content, addr)
        # sock.close()
        
    # def rcv(self, myPId):
    #     msgList = []
    #     newMsgQueue = []
    #     for item in self.messageQueue:
    #         msgList.append(item)
    #     self.messageQueue = newMsgQueue
    #     return msgList

    def prtMsgQueue(self):
        for item in self.messageQueue:
            print item

    # def operation(self):
    #     while True:
    #         print '''Please select operation:
    #         \t'send') send a message to a host;
    #         \t'prt') print all received messages
    #         \t'q') logout peer.'''
    #         option = raw_input("Your command: ")
            
    #         if option == 'send':
    #             host = raw_input("hostname: ")
    #             content = raw_input("content that you want to send: ")
    #             self.send(host, 123123, content)
    #         elif option == 'prt':
    #             print self.messageQueue
    #         elif option == 'q':
    #             break
    #         else:
    #             print "Please choose an option:"
    #             continue
