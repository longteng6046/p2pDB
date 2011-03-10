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

import sys;
import random;

from string import index;
from math import log;
from communicator import Communicator

# s: a non-negative integer
# @return: the bit string representation of s
def getBitString(s):
    if s<=1:
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

def compareIP(ip1, ip2):
    ip1 = int(ip1.split(".")[0][-2:])
    ip2 = int(ip2.split(".")[0][-2:])
    
    return ip1-ip2

class Peer:
    pId = -1
    routeTable = {}
    leafTable = {}
    neighborTable = {}
    routeMappingTable = {}
    comm = None
    messageList = []
    b = 2
    l = 8
    localHost = "0.0.0.0"
    port = 10086

    def __init__(self, pId=-1, hostIP=None, port=10086):
        self.pId = pId
        self.port = port
        for i in xrange(self.l):
            self.routeTable[i] = [None, None]
        self.routeMappingTable[None] = None
        self.comm = Communicator(self.messageList)
        self.localHost = self.getLocalHost()
        print "A peer with pId: " + str(pId) + " is created!"
        self.join(hostIP, self.port)
        #self.localOperation()

    def setPId(self, pId):
        if pId >= 0 and pId<pow(2, self.MAX_LENGTH):
            self.pId = pId;
        else:
            print "invalid peer id, default to -1..."
            self.pId = -1;

    #
    def setRouteTable(self, routeTable, routeMappingTable):
        self.routeTable = routeTable
        self.routeMappingTable = routeMappingTable

    def getRouteTable(self):
        return self.routeTable

    # neighborTable: a dict data type
    def setNeighborTable(self, neighborTable=None):
        self.neighborTable = neighborTable

    def getNeighborTable(self):
        return self.neighborTable

    # leafTable: a dict data type
    def setLeafTable(self, leafTable):
        self.leafTable = leafTable
        
    def getLeafTable(self):
        return self.leafTable

    def setPId(self, pId):
        if pId >= 0 and pId<pow(2, self.MAX_LENGTH):
            self.pId = pId;
        else:
            print "invalid peer id, default to -1..."
            self.pId = -1;

    def getPId(self):
        return self.pId


    def getLocalHost(self):
        print "hey" 
        return None

    def getMessage(): #return the first message in the message queue, and delete it.
        return None

    def getPId(self):
        return self.pId

    def join(self, hostIP, port):
        self.send(hostIP, port, "join" + "\t" + "0" + "\t" + str(self.localHost) + "\t" + str(self.pId))

    def serializeTable(self, tableName, routeTableIndex=0):
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
    
    def deserializeTable(self, string):
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

            self.leafTable = tempLeafTable
            return "leaf", tempLeafTable
        
        elif tableName=="neighbor":
            tempNeighborTable = {}
            
            for i in xrange((len(tokens)-1)/2):
                tempNeighborTable[int(tokens[i*2+1])] = tokens[i*2+2]
            
            self.neighborTable = tempNeighborTable   
            return "neighbor", tempNeighborTable
        else:
            print "undefined table type..."
            #return None, None

    def addNewNode(self, peerIP, peerID):
        peerID = int(peerID)
        
        self.leafTable[peerID] = peerIP
        if len(self.leafTable.keys()) > pow(2, self.b+1):
            distance = 0
            furthestID = self.pId
            for key in self.leafTable.keys():
                if abs(key, self.pId) > distance:
                    furthestID = key
                    distance = abs(key, self.pId)
            del self.leafTable[furthestID]
        
        cmsb = getCommonMSB(peerID, self.pId, self.l)
        lBit = int(getBitStringToLength(peerID, self.l)[len(cmsb)])
        assert lBit==0 or lBit==1
        self.routeTable[len(cmsb)][lBit] = peerID
        self.routeMappingTable[peerID] = peerIP
        
        self.neighborTable[peerID] = peerIP
        if len(self.neighborTable.keys())>pow(2, self.b+1):
            distance = 0
            furthestID = self.pId
            for key in self.neighborTable.keys():
                if abs(compare(self.neighborTable[key], self.localHost)) > distance:
                    furthestID = key
                    distance = abs(compare(self.neighborTable[key], self.localHost))
            del self.neighborTable[furthestID]

    def terminate(self):
        print str(self.pId) + " is terminating ..."

    def leave(self):
        print str(self.pId) + " has left the system."

    def stablize(self):
        print "stablize"

    def route(self, key):
        if key == self.pId:
            return "find", self.pId, self.localHost
        
        if abs(key-self.pId) < pow(2, self.b):
            closestPId = self.findClosestPID(key, self.leafTable.keys())
            
        return "contact", closestPId, self.leafTable[closestPId]
        
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
            return "find", self.pId, self._ip

    def findClosestPID(self, key, pIdList):
        closestPId = -pow(2, self.MAX_LENGTH);
            
        for peerId in pIdList:
            if abs(peerId-key) <= abs(closestPId-key):
                closestPId = peerID
        
        return closestPId

    def send(self, hostIP, port, message=None):
        self.comm.send(hostIP, port, message)

    def printMessage(self):
        for item in self.messageList:
            print item
    def prtMsgQueue(self):
        self.comm.prtMsgQueue()

    def localOperation(self):
        print "*** Welcome to peer " + str(self.pId) + '! ***'
        while (True):
            print '''Please select operation:
            \t'l') volunteerly leave the network;
            \t't') terminate without telling anyone;
            \t'send') send a message;
            \t'rcv') receive all messages;
            \t'prt') print all messages;
            \t'allMsg') print all flying messages;
            \t'r') search a key
            \t'q') logout peer.'''

            option = raw_input("Your command: ")
            if option == 'l':
                self.leave()
                return
            elif option == 't':
                self.terminate()
                return
            elif option == 'send':
                hostIP = raw_input("Please input the objective hostname: ")
                message = raw_input("Please input the message: ")
                self.send(hostIP, 10086, message)
                print "Message has been sent."
            elif option == 'rcv':
                self.rcv()
                print str(len(self.messageList)) + " new message(s)!"
            elif option == 'prt':
                self.printMessage()
            elif option == 'q':
                return
            else:
                print "Please choose an option:"
                continue


            
def testPeerSerializeTables():
    p = Peer("test");
    
    leafTable = {}
    leafTable[2]="0.57.53.13"
    leafTable[3]="0.58.53.64"
    
    neighborTable = {}
    neighborTable[1]="0.1.1.1"
    neighborTable[2]="0.2.2.2"
    
    routeTable = {}
    for i in xrange(8):
        routeTable[i] = [None, None]
    routeTable[5] = [8, 9]
    routeTable[6] = [11, 12]
    
    routeMappingTable = {}
    routeMappingTable[None] = None
    routeMappingTable[8] = "4.5.6.7"
    routeMappingTable[9] = "1.2.3.4"
    routeMappingTable[11] = "3.4567"
    routeMappingTable[12] = "4.5678"
    
    p.setLeafTable(leafTable)
    p.setNeighborTable(neighborTable)
    p.setRouteTable(routeTable, routeMappingTable)
    
    print p.deserializeTable(p.serializeTable("route", 5))
    print p.deserializeTable(p.serializeTable("route", 4))
    print p.deserializeTable(p.serializeTable("leaf"))
    print p.deserializeTable(p.serializeTable("neighbor"))


if __name__ == "__main__":
    testPeerSerializeTables()
    
##############################
# Main
##############################            
            
if len(sys.argv) == 1:
    print "This is the first peer of the system."
    hostIP = ""
    port = 0
elif len(sys.argv) == 4:
    hostIP = sys.argv[2]
    port = sys.argv[3]
    print "Connecting via peer on " + ip + ":" + str(port) + "."
else:
    print "Wrong argument list."
    sys.exit(1)
pId = random.randint(0,255)
selfPeer = Peer(pId, hostIP, port)
