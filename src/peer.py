#!/usr/bin/python
#
#-------------------------------------------------------------------------------
# Filename: peer.py
# Version: 0.3
# Description:
#     Define a class Peer, having all data structures needed for a peer.
#
# Update:
#     For 0.3: send/receive is added to each peer.
#     use peer.send(objective_Peer_Id, message) to send a message;
#     use peer.rcv() to rcv all messages into local messageList.
#     received messages will not be received again by calling rcv();
#     local messageList will be overritten by multiple call of rcv().
#-------------------------------------------------------------------------------

from collections import defaultdict;
from string import index;
from math import log;

# s: a non-negative integer
# @return: the bit string representation of s
def getBitString(s):
    return str(s) if s<=1 else getBitString(s>>1) + str(s&1)

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

class Peer(object):
    messageList = []

    # pID: a peer ID
    # comm: a communicator instances
    def __init__(self, pId=-1, comm=None, ip = "0.0.0.0", b=2, l=8):
        self._MARGIN = b
        self._MAX_LENGTH = l
        
        self._pId = int(pId)
        self._ip = ip
        self._comm = comm
        print "A peer with pId: " + str(pId) + " is created!"

    #
    def _initialize(self, routeTable=None, leafTable=None, neighborTable=None, routeMappingTable=None):
        self._routeTable = routeTable
        self._routeMappingTable = routeMappingTable
        self._leafTable = leafTable
        self._neighborTable = neighborTable
#        self._mappings = mappings

    #
    def setRouteTable(self, routeTable):
        self._routeTable = routeTable

    def getRouteTable(self):
        return self._routeTable

    # neighborTable: a dict data type
    def setNeighborTable(self, neighborTable=None):
        self._neighborTable = neighborTable

    def getNeighborTable(self):
        return self._neighborTable

    # leafTable: a dict data type
    def setLeafTable(self, leafTable):
        self._leafTable = leafTable
        
    def getLeafTable(self):
        return self._leafTable

    def setPId(self, pId):
        if pId >= 0 and pId<pow(2, self.MAX_LENGTH):
            self._pId = pId;
        else:
            print "invalid peer id, default to -1..."
            self._pId = -1;

    def getPId(self):
        return self._pId
    
    

    def createRouteTable(self):
        routeTable = defaultdict(dict)
        
    def mergeRouteTable(self, newRouteTable):
        return

    def join(self, contactPeer):
        if contactPeer == None:
            print "The contact Peer has not been specified."
            return
        print str(self._pId) + " is joining peer " + str(self.contactPeer.getPId())
        
        key = self._pId
        msgList = []
        peerIDList = []
        peerIPList = []
        while True:
            # TODO: contact the peer with some ip address
            msg, peerID, peerIP = contactPeer.route(key)
            msgList.append(msg)
            peerIDList.append(peerID)
            peerIPList.append(peerIP)
            
            if msg=="find":
                break
            
            # TODO: contact the new peer with this ip address
            key = peerID
            

    def terminate(self):
        print str(self._pId) + " is terminating ..."

    def leave(self):
        # TODO: send messages to some node
        print str(self._pId) + " has left the system."

    def stablize(self):
        print "stablize"

    def route(self, key):
        if key == self._pId:
            return "find", self._pId
        
        if abs(key-self._pId) < pow(2, self._MARGIN):
            closestPId = self.findClosestPID(key, self._leafTable.keys())
            
        return "contact", closestPId, self._leafTable[closestPId]
        
        commonBitString = getCommonMSB(self._pId, key, self._MAX_LENGTH)
        lBit = int(getBitStringToLength(key, _L)[len(commonBitString)])
        if self._routeTable[len(commonBitString)][lBit] != None:
            return "contact", self._routeTable[len(commonBitString)][lBit], self._routeMappingTable[self._routeTable[len(commonBitString)][lBit]]

        CPIDL = self.findClosestPID(key, self._leafTable.keys())
        CPIDN = self.findClosestPID(key, self._neighborTable.keys())
        CPIDR = self.findClosestPID(key, self._routeMappingTable.keys())
        
        if abs(CPIDL-key) <= abs(self._pId-key) and abs(CPIDL-key) <= abs(CPIDN-key) and abs(CPIDL-key) <= abs(CPIDR-key):
            return "contact", CPIDL, self._leafTable[CPIDL]
        elif abs(CPIDN-key) <= abs(self._pId-key) and abs(CPIDN-key) <= abs(CPIDL-key) and abs(CPIDN-key) <= abs(CPIDR-key):
            return "contact", CPIDN, self._neighborTable[CPIDN]
        elif abs(CPIDR-key) <= abs(self._pId-key) and abs(CPIDR-key) <= abs(CPIDL-key) and abs(CPIDR-key) <= abs(CPIDN-key):
            return "contact", CPIDR, self._routeMappingTable[CPIDR]
        else:
            return "find", self._pId, self._ip

    def findClosestPID(self, key, pIdList):
        closestPId = -pow(2, self.MAX_LENGTH);
            
        for peerId in pIdList:
            if abs(peerId-key) <= abs(closestPId-key):
                closestPId = peerID
        
        return closestPId

    def send(self, objPId, message):
        self._comm.send(self._pId, objPId, message)

    def rcv(self):
        self.messageList = self._comm.rcv(self._pId)

    def printMessage(self):
        for item in self.messageList:
            print '"' + str(item[2]) + '" from peer ' + str(item[0]) + '.'
    def prtMsgQueue(self):
        self._comm.prtMsgQueue()

    def localOperation(self):
        print "*** Welcome to peer " + str(self._pId) + '! ***'
        while (True):
            print '''Please select operation:
            \t'j') join the contacted peer;
            \t'l') volunteerly leave the network;
            \t't') terminate without telling anyone;
            \t'send') send a message;
            \t'rcv') receive all messages;
            \t'prt') print all messages;
            \t'allMsg') print all flying messages;
            \t'r') search a key
            \t'q') logout peer.'''

            option = raw_input("Your command: ")

            if option == 'j':
                self.join(None)
            elif option == 'l':
                self.leave()
                return
            elif option == 't':
                self.terminate()
                return
            elif option == 'send':
                objId = int(raw_input("Please input the object _pId: "))
                message = raw_input("Please input the message: ")
                self.send(objId, message)
                print "Message has been sent."
            elif option == 'rcv':
                self.rcv()
                print str(len(self.messageList)) + " new message(s)!"
            elif option == 'prt':
                self.printMessage()
            elif option == 'allMsg':
                self.prtMsgQueue()
            elif option == 'r':
                key = raw_input("Please input the message: ")
                self.route(int(key))
            elif option == 'q':
                return
            else:
                print "Please choose an option:"
                continue
            
            
if __name__ == "__main__":
    print min(2, 45231, 5415, 6)
    cbs = getCommonMSB(171, 164, 10)
    print xrange(2)
    
    lBit = int(getBitStringToLength(164, 10)[len(cbs)])
    print getBitStringToLength(164, 10), "\t", lBit