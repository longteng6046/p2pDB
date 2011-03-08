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

class Peer:

    messageList = []

    # pID: a peer ID
    # comm: a communicator instances
    def __init__(self, pId=-1, comm=None):
        self._pId = pId
        self._comm = comm
        print "A peer with pId: " + str(pId) + " is created!"
        
    #
    def _initialize(self, routeTable=None, leafTable=None, neighborTable=None):
        self._routeTable = routeTable
        self._leafTable = leafTable
        self._neighborTable = neighborTable

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
        self._pId = pId;

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
        contactPeer.route(self._pID)

    def terminate(self):
        print str(self._pId) + " is terminating ..."

    def leave(self):
        print str(self._pId) + " has left the system."

    def stablize(self):
        print "stablize"

    def route(self, key):
        if key in self._leafTable.keys():
            return self._leafTable[key]
        print "Routing key " + key + " from peer " + str(self._pId)

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
            elif option == 'q':
                return
            else:
                print "Please choose an option:"
                continue