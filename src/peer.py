#!/usr/bin/python
# 
#-------------------------------------------------------------------------------
# Filename: peer.py
# Version: 0.31
# Description:
#     Define a class Peer, having all data structures needed for a peer.
# Update:
#     For 0.3: send/receive is added to each peer.
#     use peer.send(objective_Peer_Id, message) to send a message;
#     use peer.rcv() to rcv all messages into local messageList.
#     received messages will not be received again by calling rcv();
#     local messageList will be overritten by multiple call of rcv().
#-------------------------------------------------------------------------------

import sys
import threading
import socket
import random
import time

from string import index;
from math import log;
from copy import deepcopy;

from communicator import *
from listener import *
from processor import *
from live_checker import *

class Peer:
    pID = ""
    pIP = "0.0.0.0"
    
    # base_power defines the base of the key space, i.e., in 2^base_power space     
    base_power = 4
    # length defines the length of the key
    length = 40
    port = 12345
    
    routeTable = {}
    leafTable = {}
    neighborTable = {}
    routeMappingTable = {}
    communicator = None
    processor = None
    terminateFlag = False
    
    messageQueue = []
    stabQueue = [] # The queue to store stablizing messages from other nodes
    
    leafRange = length
    neighborRange = length
    
    msgLock = None
    deadPeerLock = None
    tableLock = None # The lock for all tables;
    stabLock = None # The lock for stabQueue.
    
    def __init__(self, hostIP = None):
        from id_ops import getSha1
        self.pIP = self.getIP()
        self.pID = getSha1(self.pIP)
        
        print "This peer runs on pIP: " + self.getIP() + " with pID " + self.pID + "."

        for i in xrange(self.length):
            self.routeTable[i] = [None for i in range(pow(2, self.base_power))]
        self.routeMappingTable[None] = None
        
        self.msgLock = threading.Lock()
        self.deadPeerLock = threading.Lock()
        self.tableLock = threading.Lock()
        self.stabLock = threading.Lock()

        self.processor = Processor(self)
        self.processor.setDaemon(True)
        self.processor.start()

        self.livechecker = LiveChecker(self)
        self.livechecker.setDaemon(True)
        self.livechecker.start()
        
        self.communicator = Communicator(self)
        
        if hostIP != None:
            self.join(hostIP, joinPort)
        
        self.localOperation()

    def setPId(self, pID):
        assert len(pID)==self.length
        self.pID = pID;
        
    def findId(self, targetIP):
        nTable = deepcopy(self.neighborTable)
        lTable = deepcopy(self.leafTable)
        rTable = deepcopy(self.routeMappingTable)

        for item in nTable:
            if nTable[item] == targetIP:
                return item
        for item in lTable:
            if lTable[item] == targetIP:
                return item
        for item in rTable:
            if rTable[item] == targetIP:
                return item
        return None
            
    def getMessage(self):
        #return the first message in the message queue, and delete it.
        # check lock first
        if self.messageQueue == None:
            return None
        if self.msgLock.acquire():
            if len(self.messageQueue) == 0:
                self.msgLock.release()
                return None
            else:
                result = self.messageQueue[0]
                del self.messageQueue[0]
                self.msgLock.release()
                return result

    def getID(self):
        return self.pID
    
    # @deprecated: please use peer ip or id for identification purpose
    def getName(self):
        return socket.gethostname()
    
    def getIP(self):
        from ip_ops import getIP
        return getIP('eth0')

    def serialize(self, tableName, routeTableIndex=0):
        string = tableName
        if tableName=="route":
            string += "\t" + str(routeTableIndex)
            for i in pow(2, self.base_power):
                string += "\t" + str(self.routeTable[routeTableIndex][i]) + "\t" + str(self.routeMappingTable[self.routeTable[routeTableIndex][i]])
        elif tableName=="leaf":
            for key in self.leafTable.keys():
                string += "\t" + str(key) + "\t" + str(self.leafTable[key])
        elif tableName=="neighbor":
            for key in self.neighborTable.keys():
                string += "\t" + str(key) + "\t" + str(self.neighborTable[key])
        else:
            print "undefined table type..."
            return None
            
        return string.strip()
    
    def deserialize(self, string):
        tokens = string.split("\t")
        tableName = tokens[0].strip()
        
        if tableName=="route":
            tempRouteTable = []
            tableLevel = int(tokens[1])
            if tableLevel<0 or tableLevel>=self.length:
                print "invalid route table level..."
                return None, None
            return tableLevel, tokens[2:]
        
        elif tableName=="leaf":
            tempLeafTable = {}
            for i in xrange((len(tokens)-1)/2):
                tempLeafTable[int(tokens[i*2+1])] = tokens[i*2+2]
            return "leaf", tempLeafTable
        
        elif tableName=="neighbor":
            tempNeighborTable = {}
            for i in xrange((len(tokens)-1)/2):
                tempNeighborTable[int(tokens[i*2+1])] = tokens[i*2+2]
            return "neighbor", tempNeighborTable
        
        else:
            print "undefined table type..."
            #return None, None

    def addNewNode(self, peerIP, peerID):
        from ip_ops import getExpression
        from id_ops import getHexDifference
                
        assert len(peerID) == self.length
                
#        if self.tableLock.acquire():
        if abs(getHexDifference(peerID, self.pID))<=self.leafRange/2:
            self.leafTable[peerID] = peerIP
            
#            self.leafTable[peerID] = peerIP
#            if len(self.leafTable.keys()) > self.leafRange():
#                distance = 0
#                furthestID = self.pID
#                for key in self.leafTable.keys():
#                    if abs(key, self.pID) > distance:
#                        furthestID = key
#                        distance = abs(key, self.pID)
#                del self.leafTable[furthestID]
        
        cmsb = getCommonMSB(peerID, self.pID, self.length)
        lBit = int(expendLeftToLength(peerID, self.length)[len(cmsb)], 16)
        #lBit = int(getBitStringToLength(peerID, self.length)[len(cmsb)])
        assert lBit>=0 and lBit<2**self.base_length
        
        if self.routeTable[len(cmsb)][lBit]==None:
            self.routeTable[len(cmsb)][lBit] = peerID
            self.routeMappingTable[peerID] = peerIP                
        assert len(self.routeTable[len(cmsb)][lBit])==self.length
        
        if abs(getHexDifference(self.routeTable[len(cmsb)][lBit], self.pID))<abs(getHexDifference(peerID, self.pID)):
            del self.routeMappingTable[self.routeTable[len(cmsb)][lBit]]
            self.routeTable[len(cmsb)][lBit] = peerID
            self.routeMappingTable[peerID] = peerIP

        self.neighborTable[peerID] = peerIP
        if len(self.neighborTable.keys())>self.neighborRange:
            distance = 0
            furthestID = self.pID
            for key in self.neighborTable.keys():
                if abs(compare(getExpression(self.neighborTable[key]), getExpression(self.pIP))) > distance:
                    furthestID = key
                    distance = abs(compare(getExpression(self.neighborTable[key]), getExpression(self.pIP)))
            del self.neighborTable[furthestID]
            
        assert len(self.neighborTable)<=self.neighborRange
            
        #self.tableLock.release()
        
    def stablize(self, peerId):
        if peerId in self.leafTable:
            del self.leafTable[peerId]
        if peerId in self.neighborTable:
            del self.neighborTable[peerId]
        if peerId in self.routeMappingTable:
            del self.routeMappingTable[peerId]
            
            for item in self.routeTable:
                members = self.routeTable[item]
                if peerId in members:
                    members[members.index(peerId)] = None

    def join(self, pIP, port):
        result = self.send(pIP, port, "join" + "\t" + "0" + "\t" + str(self.pIP) + "\t" + str(self.pID))
#        if result != True:
#            print "successfully join"

    def find(self, key):
        self.send(self.pIP, self.port, "route" + "\t" + str(0) + "\t" + self.pIP + "\t" + str(self.pID) + "\t" + key)

    def route(self, key):
        from id_ops import findClosestID
        
        assert len(key)==self.length
        
        if key == self.pID:
            return "find", self.pID, self.pIP
        
        if len(self.leafTable)!=0 and abs(getHexDifference(key, self.pID)) <= self.leafRange/2:
            (closestPId, closestPIp) = findClosestID(key, self.leafTable.keys())
            if abs(getHexDifference(key, closestPId))<abs(getHexDifference(key, self.pID)):
                return "contact", closestPId, self.leafTable[closestPId]
            else:
                return "find", self.pID, self.pIP
        
        commonBitString = getCommonMSB(self.pID, key, self.length)
        lBit = int(expendLeftToLength(peerID, self.length)[len(cmsb)], 16)
        assert lBit>=0 and lBit<2**self.base_length
        
        if self.routeTable[len(commonBitString)][lBit] != None:
            return "contact", self.routeTable[len(commonBitString)][lBit], self.routeMappingTable[self.routeTable[len(commonBitString)][lBit]]

        (CPIDL, CPIPL) = findClosestID(key, self.leafTable.keys())
        (CPIDN, CPIPN)= findClosestID(key, self.neighborTable.keys())
        (CPIDR, CPIPR) = findClosestID(key, self.routeMappingTable.keys())
        
        if abs(CPIDL-key) < abs(self.pID-key) and abs(CPIDL-key) < abs(CPIDN-key) and abs(CPIDL-key) < abs(CPIDR-key):
            return "contact", CPIDL, self.leafTable[CPIDL]
        elif abs(CPIDN-key) < abs(self.pID-key) and abs(CPIDN-key) < abs(CPIDL-key) and abs(CPIDN-key) < abs(CPIDR-key):
            return "contact", CPIDN, self.neighborTable[CPIDN]
        elif abs(CPIDR-key) < abs(self.pID-key) and abs(CPIDR-key) < abs(CPIDL-key) and abs(CPIDR-key) < abs(CPIDN-key):
            return "contact", CPIDR, self.routeMappingTable[CPIDR]
        else:
            return "find", self.pID, self.pIP

    def send(self, destination, sendPort, content):
        return self.communicator.send(destination, sendPort, content)
        # # socket setting 
        # buf = 1024 * 1024
        # addr = (destination, sendPort)
        
        # family, socktype, proto, garbage, address = socket.getaddrinfo(destination, sendPort)[0]
        # # sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # sock = socket.socket(family, socktype, proto)
        # try:
        #     # print "addr: ", addr, "peer"
        #     sock.connect(address)
        # except Exception:
        #     print "Exception: ", Exception
        #     return False
        # # print "overTry?"
        # sock.send(content)
        # sock.close()

    def terminate(self):
        print str(self.pID) + " is terminating ..."
        try:
            self.processor.kill()        
            self.communicator.mylistener.kill()
            self.livechecker.kill()

        except:
            None
        
        sys.exit(0)
        
    def leave(self):
        print str(self.pID) + " has left the system."
        self.terminate()

    def printMessage(self):
        for item in self.messageQueue:
            print "message: ", item
    
    def prtMsgQueue(self):
        self.communicator.prtMsgQueue()

    def localOperation(self):
        print "*** Welcome to peer " + str(self.pID) + '! ***'
        while (True):
            print '''Please select operation:
            \t'r') To search the host of a key;
            \t'p') To print the tables of Pastry;
            \t'id') To check the ID of the current peer;
            \t't') To quit/terminate the current peer.'''

            option = raw_input("Your command: ")
            if option == 't':
                self.terminate()
                return
            elif option == 'r':
                key = raw_input("Please input the key you want to query:")
                self.route(key)
                print self.route(key)
            elif option == 'p':
                print "\nLeafTable:"
                stream = ''
                for item in self.leafTable: 
                    print "id: ", item, " hostname: ", self.leafTable[item]
                print "\nRouteTable:"
                tmp = deepcopy(self.routeTable)
                for item in tmp:
                    print item, ": ", tmp[item]
                print "\nNeighborTable:"                
                for item in self.neighborTable:
                    print "id: ", item, " hostname:  ", self.neighborTable[item]
                    print
            elif option == 'id':
                print "The id of the current peer: ", self.getID()
            else:
                print "Please choose an option:"
                continue

if __name__ == "__main__":
    selfPeer = Peer(None)
