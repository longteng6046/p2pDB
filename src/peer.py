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
from LifeChecker import *
from copy import deepcopy

# s: a non-negative integer
# @return: the bit string representation of s
def getBitString(s):
    if s <= 1:
        return str(s)
    else:
        return getBitString(s>>1) + str(s&1)

# s: a non-negative integer
# maxLength: the maximum length of the retrieved bit string
# @return: the bit string representation of s
def getBitStringToLength(s, maxLength):
    bs = getBitString(s)
    
    assert len(bs) <= maxLength
    
    for i in xrange(maxLength-len(bs)):
        bs = "0" + bs
     
    return bs

# int1, int2: two non-negative integer
# return the common MSBs from int1 and int2
def getCommonMSB(int1, int2, maxLength):
    if int1<int2:
        temp = int1;
        int1 = int2;
        int2 = temp;
    
    lint1 = len(getBitString(int1))
    lint2 = len(getBitString(int2))
    
    bs=""
    for i in xrange(maxLength-lint1):
        bs = "0" + bs
#    print getBitString(int1), "\t", getBitString(int2), "\t", bs
    
    if lint1 == lint2:
        for i in xrange(lint2):
#            print i, "\t", int1/2, "\t", int2/2
            if int1/(int(pow(2, lint2-i-1)))==int2/(int(pow(2, lint2-i-1))):
                bs = bs + str(int1/(int(pow(2, lint2-i-1))))
                int1 = int1%(int(pow(2, lint2-i-1)))
                int2 = int2%(int(pow(2, lint2-i-1)))
            else:
                break
    return bs

# def compareIP(ip1, ip2):
    # ip1 = int(ip1.split(".")[0][-2:])
    # ip2 = int(ip2.split(".")[0][-2:])
    # return ip1-ip2

class Peer:
    pId = -1
    routeTable = {}
    leafTable = {}
    neighborTable = {}
    routeMappingTable = {}
    comm = None
    processor = None
    terminateFlag = False
    
    messageQueue = []
    deadPeerQueue = []
    stabQueue = [] # The queue to store stablizing messages from other nodes
    
    b = 2
    l = 40

    leafRange = 32
    neighborRange = 16

    localHost = "0.0.0.0"
    listenPort = 12345
    sendPort = 12345
    port = 12345

    msgLock = None
    deadPeerLock = None
    tableLock = None # The lock for all tables;
    stabLock = None # The lock for stabQueue.
    
    def __init__(self, pId, host = None, joinPort = 12345):
        print "This peer runs on host: " + self.getLocalHost() + "."
        self.pId = pId
        self.localHost = self.getLocalHost()

        self.comm = Communicator(self)

        for i in xrange(self.l):
            self.routeTable[i] = [None, None]
        self.routeMappingTable[None] = None

        # print "A peer with pId: " + str(pId) + " is created!"

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

        if host != None:
            self.join(host, joinPort)
        
        self.localOperation()





    def setPId(self, pId):
        if pId >= 0 and pId<pow(2, self.l):
            self.pId = pId;
        else:
            print "invalid peer id, default to -1..."
            self.pId = -1;

    def findId(self, hostname):
        nTable = deepcopy(self.neighborTable)
        lTable = deepcopy(self.leafTable)
        rTable = deepcopy(self.routeMappingTable)

        for item in nTable:
            if nTable[item] == hostname:
                return item
        for item in lTable:
            if lTable[item] == hostname:
                return item
        for item in rTable:
            if rTable[item] == hostname:
                return item
        return None
            
    def getLocalHost(self):
        return socket.gethostname()

    def getMessage(self): #return the first message in the message queue, and delete it.
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



    def getPId(self):
        return self.pId

    def join(self, hostIP, port):
        result = self.send(hostIP, port, "join" + "\t" + "0" + "\t" + str(self.localHost) + "\t" + str(self.pId))
#        if result != True:
#            print "successfully join"

    def serialize(self, tableName, routeTableIndex=0):
        string = tableName
        if tableName=="route":
            string += "\t" + str(routeTableIndex) + "\t" + str(self.routeTable[routeTableIndex][0]) + "\t" + str(self.routeMappingTable[self.routeTable[routeTableIndex][0]]) + "\t" + str(self.routeTable[routeTableIndex][1]) + "\t" + str(self.routeMappingTable[self.routeTable[routeTableIndex][1]])
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
            tableLevel = int(tokens[1])
            
            if tableLevel<0 or tableLevel>=self.l:
                print "invalid route table level..."
                return None, None
            
            #self.routeTable[tableLevel] = [int(tokens[2]), int(tokens[4])]
            #self.routeMappingTable[int(tokens[2])] = tokens[3]
            #self.routeMappingTable[int(tokens[4])] = tokens[5]
            
            if tokens[2]!="None":
                tokens[2] = int(tokens[2])
            if tokens[4]!="None":
                tokens[4] = int(tokens[4])
            return tableLevel, [tokens[2], tokens[3], tokens[4], tokens[5]]
        
        elif tableName=="leaf":
            tempLeafTable = {}
            for i in xrange((len(tokens)-1)/2):
                tempLeafTable[int(tokens[i*2+1])] = tokens[i*2+2]

            #self.leafTable = tempLeafTable
            return "leaf", tempLeafTable
        
        elif tableName=="neighbor":
            tempNeighborTable = {}
            
            for i in xrange((len(tokens)-1)/2):
                tempNeighborTable[int(tokens[i*2+1])] = tokens[i*2+2]
            
            #self.neighborTable = tempNeighborTable   
            return "neighbor", tempNeighborTable
        else:
            print "undefined table type..."
            #return None, None

    def addNewNode(self, peerIP, peerID):
        peerID = int(peerID)
        
#        if self.tableLock.acquire():
        if abs(peerID-self.pId)<=self.leafRange/2:
            self.leafTable[peerID] = peerIP
            
#            self.leafTable[peerID] = peerIP
#            if len(self.leafTable.keys()) > self.leafRange():
#                distance = 0
#                furthestID = self.pId
#                for key in self.leafTable.keys():
#                    if abs(key, self.pId) > distance:
#                        furthestID = key
#                        distance = abs(key, self.pId)
#                del self.leafTable[furthestID]
        
        cmsb = getCommonMSB(peerID, self.pId, self.l)
        lBit = int(getBitStringToLength(peerID, self.l)[len(cmsb)])
        assert lBit==0 or lBit==1

        if self.routeTable[len(cmsb)][lBit]==None:
            self.routeTable[len(cmsb)][lBit] = peerID
            self.routeMappingTable[peerID] = peerIP
                
        if abs(self.routeTable[len(cmsb)][lBit]-self.pId)<abs(peerID-self.pId):
            del self.routeMappingTable[self.routeTable[len(cmsb)][lBit]]
            self.routeTable[len(cmsb)][lBit] = peerID
            self.routeMappingTable[peerID] = peerIP
        
        self.neighborTable[peerID] = peerIP
#            if len(self.neighborTable.keys())>pow(2, self.b+1):
        if len(self.neighborTable.keys())>self.neighborRange:
            distance = 0
            furthestID = self.pId
            for key in self.neighborTable.keys():
                if abs(compare(self.neighborTable[key], self.localHost)) > distance:
                    furthestID = key
                    distance = abs(compare(self.neighborTable[key].split('.'), self.localHost.split('.')))
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
            
            # cmsb = getCommonMSB(peerId, self.pId, self.l)
            # lBit = int(getBitStringToLength(peerId, self.l)[len(cmsb)])
            # print "cmsb:", cmsb, " lBit:", lBit, "len: ", len(cmsb)
            # self.routeTable[len(cmsb)][lBit] = None


    
    def terminate(self):
        print str(self.pId) + " is terminating ..."
        try:
            self.processor.kill()        
            self.comm.mylistener.kill()
            self.lifechecker.kill()

        except:
            None
        
        sys.exit(0)

    def leave(self):
        print str(self.pId) + " has left the system."

    def find(self, key):
        self.send(self.localHost, self.port, "route" + "\t" + str(0) + "\t" + self.localHost + "\t" + str(self.pId) + "\t" + key)

    def route(self, key):
        key = int(key)
        if key == self.pId:
            return "find", self.pId, self.localHost
        
        if len(self.leafTable)!=0 and abs(key-self.pId) <= self.leafRange/2:
            closestPId = self.findClosestPID(key, self.leafTable.keys())
            if abs(key-closestPId)<abs(key-self.pId):
                return "contact", closestPId, self.leafTable[closestPId]
            else:
                return "find", self.pId, self.localHost
        
        commonBitString = getCommonMSB(self.pId, key, self.l)
        lBit = int(getBitStringToLength(key, self.l)[len(commonBitString)])
        assert lBit==0 or lBit==1
        if self.routeTable[len(commonBitString)][lBit] != None:
            return "contact", self.routeTable[len(commonBitString)][lBit], self.routeMappingTable[self.routeTable[len(commonBitString)][lBit]]

        CPIDL = self.findClosestPID(key, self.leafTable.keys())
        CPIDN = self.findClosestPID(key, self.neighborTable.keys())
        CPIDR = self.findClosestPID(key, self.routeMappingTable.keys())
        
        if abs(CPIDL-key) < abs(self.pId-key) and abs(CPIDL-key) < abs(CPIDN-key) and abs(CPIDL-key) < abs(CPIDR-key):
            return "contact", CPIDL, self.leafTable[CPIDL]
        elif abs(CPIDN-key) < abs(self.pId-key) and abs(CPIDN-key) < abs(CPIDL-key) and abs(CPIDN-key) < abs(CPIDR-key):
            return "contact", CPIDN, self.neighborTable[CPIDN]
        elif abs(CPIDR-key) < abs(self.pId-key) and abs(CPIDR-key) < abs(CPIDL-key) and abs(CPIDR-key) < abs(CPIDN-key):
            return "contact", CPIDR, self.routeMappingTable[CPIDR]
        else:
            return "find", self.pId, self.localHost

    def findClosestPID(self, key, pIdList):
        closestPId = -pow(2, 2*self.l);
            
        for peerId in pIdList:
            if peerId==None:
                # pIdList is empty
                continue
            if abs(peerId-key) <= abs(closestPId-key):
                closestPId = peerId
        
        return closestPId

    def send(self, host, sendPort, content):
        return self.comm.send(host, sendPort, content)
        # # socket setting 
        # buf = 1024 * 1024
        # addr = (host, sendPort)
        
        # family, socktype, proto, garbage, address = socket.getaddrinfo(host, sendPort)[0]
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
        self.comm.prtMsgQueue()

    def localOperation(self):
        print "*** Welcome to peer " + str(self.pId) + '! ***'
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
                print "The id of the current peer: ", self.getPId()
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
    
pId = random.randint(0,255)
            
if len(sys.argv) == 1:
    print "\n*************** This is the first peer of the system. ***************\n"
    host = None
    selfPeer = Peer(pId, host)    
elif len(sys.argv) == 2:
    host = sys.argv[1]
    print "Connecting via peer on " + host + "."
    selfPeer = Peer(pId, host)

else:
    print "Wrong argument list."
    sys.exit(1)


