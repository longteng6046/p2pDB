#!/usr/bin/python
# 
#-------------------------------------------------------------------------------
# Filename: peer.py
# Version: 0.3
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
# from collections import defaultdict;
from string import index;
from math import log;

from communicator import *
from listener import *
from processor import *
from lifechecker import *
from copy import deepcopy
from util import *

class Peer:
    pID = -1
    pIP = "0.0.0.0"
    listenPort = 12345
    sendPort = 12345
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
    
    # base_power defines the base of the key space, i.e., in 2^base_power space     
    base_power = 4
    # length defines the length of the key
    length = 40

    leafRange = length
    neighborRange = length
    
    msgLock = None
    deadPeerLock = None
    tableLock = None # The lock for all tables;
    stabLock = None # The lock for stabQueue.
    
    def __init__(self, pID, hostIP = None, joinPort = 12345):
        print "This peer runs on pIP: " + self.getIP() + " with pID " + pID + "."
        self.pID = pID
        self.pIP = self.getIP()

        self.communicator = Communicator(self)

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

        self.lifechecker = LifeChecker(self)
        self.lifechecker.setDaemon(True)
        self.lifechecker.start()

        if hostIP != None:
            self.join(hostIP, joinPort)
        
        self.localOperation()

    def setPId(self, pID):
        if len(pID)==self.length:
            self.pID = pID;
        else:
            print "invalid peer id, default to 'invalid peer index'..."
            self.pID = getSha1("invalid peer index");

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
    
    # TODO: change the method
    def getIP(self):
        return socket

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
        assert len(peerID) == self.length
                
#        if self.tableLock.acquire():
        if abs(getDifference(hex2bin(peerID), hex2bin(self.pID)))<=self.leafRange/2:
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
        
        if abs(getDifference(hex2bin(self.routeTable[len(cmsb)][lBit])-hex2bin(self.pID)))<abs(getDifference(hex2bin(peerID)-hex2bin(self.pID))):
            del self.routeMappingTable[self.routeTable[len(cmsb)][lBit]]
            self.routeTable[len(cmsb)][lBit] = peerID
            self.routeMappingTable[peerID] = peerIP
        
        self.neighborTable[peerID] = peerIP
        if len(self.neighborTable.keys())>self.neighborRange:
            distance = 0
            furthestID = self.pID
            for key in self.neighborTable.keys():
                if abs(compare(self.neighborTable[key], self.pIP)) > distance:
                    furthestID = key
                    distance = abs(compare(self.neighborTable[key].split('.'), self.pIP.split('.')))
            del self.neighborTable[furthestID]
            
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
            
            # cmsb = getCommonMSB(peerId, self.pID, self.length)
            # lBit = int(getBitStringToLength(peerId, self.length)[len(cmsb)])
            # print "cmsb:", cmsb, " lBit:", lBit, "len: ", len(cmsb)
            # self.routeTable[len(cmsb)][lBit] = None

    def terminate(self):
        print str(self.pID) + " is terminating ..."
        try:
            self.processor.kill()        
            self.communicator.mylistener.kill()
            self.lifechecker.kill()

        except:
            None
        
        sys.exit(0)

    def join(self, pIP, port):
        result = self.send(pIP, port, "join" + "\t" + "0" + "\t" + str(self.pIP) + "\t" + str(self.pID))
#        if result != True:
#            print "successfully join"

    def leave(self):
        print str(self.pID) + " has left the system."

    def find(self, key):
        self.send(self.pIP, self.port, "route" + "\t" + str(0) + "\t" + self.pIP + "\t" + str(self.pID) + "\t" + key)

    def route(self, key):
        key = int(key)
        if key == self.pID:
            return "find", self.pID, self.pIP
        
        if len(self.leafTable)!=0 and abs(key-self.pID) <= self.leafRange/2:
            closestPId = self.findClosestPID(key, self.leafTable.keys())
            if abs(key-closestPId)<abs(key-self.pID):
                return "contact", closestPId, self.leafTable[closestPId]
            else:
                return "find", self.pID, self.pIP
        
        commonBitString = getCommonMSB(self.pID, key, self.length)
        lBit = int(getBitStringToLength(key, self.length)[len(commonBitString)])
        assert lBit==0 or lBit==1
        if self.routeTable[len(commonBitString)][lBit] != None:
            return "contact", self.routeTable[len(commonBitString)][lBit], self.routeMappingTable[self.routeTable[len(commonBitString)][lBit]]

        CPIDL = self.findClosestPID(key, self.leafTable.keys())
        CPIDN = self.findClosestPID(key, self.neighborTable.keys())
        CPIDR = self.findClosestPID(key, self.routeMappingTable.keys())
        
        if abs(CPIDL-key) < abs(self.pID-key) and abs(CPIDL-key) < abs(CPIDN-key) and abs(CPIDL-key) < abs(CPIDR-key):
            return "contact", CPIDL, self.leafTable[CPIDL]
        elif abs(CPIDN-key) < abs(self.pID-key) and abs(CPIDN-key) < abs(CPIDL-key) and abs(CPIDN-key) < abs(CPIDR-key):
            return "contact", CPIDN, self.neighborTable[CPIDN]
        elif abs(CPIDR-key) < abs(self.pID-key) and abs(CPIDR-key) < abs(CPIDL-key) and abs(CPIDR-key) < abs(CPIDN-key):
            return "contact", CPIDR, self.routeMappingTable[CPIDR]
        else:
            return "find", self.pID, self.pIP

    def findClosestPID(self, key, pIdList):
        closestPId = -pow(2, 2*self.length);
            
        for peerId in pIdList:
            if peerId==None:
                # pIdList is empty
                continue
            if abs(peerId-key) <= abs(closestPId-key):
                closestPId = peerId
        
        return closestPId

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
                print "neighborTable:"                
                for item in self.neighborTable:
                    print "id: ", item, " hostname:  ", self.neighborTable[item]
                    print
            elif option == 'id':
                print "The id of the current peer: ", self.getID()
            else:
                print "Please choose an option:"
                continue

##############################
# Main
##############################            

print "\n********************************** Welcome to DHT! **********************************"
print "Please use './pastry hostname' to join Pastry, or './pastry' to start a new Pastry network."
            

if len(sys.argv) != 2 and len(sys.argv) != 1:
    print "\tFormat error! use './pastry hostname' to join Pastry, or './pastry' to start a new Pastry network."
    sys.exit(1)

if len(sys.argv) == 2 and len(sys.argv[1].split('.')) != 4:
    print "\tFormat error! Please use IP or hostname (***.***.***.***) to specify the Pastry node you want to contact."
    sys.exit(1)
    
pID = random.randint(0,255)
            
if len(sys.argv) == 1:
    print "\n*************** This is the first peer of the system. ***************\n"
    host = None
    selfPeer = Peer(pID, host)    
elif len(sys.argv) == 2:
    host = sys.argv[1]
    print "Connecting via peer on " + host + "."
    selfPeer = Peer(pID, host)

else:
    print "Wrong argument list."
    sys.exit(1)


