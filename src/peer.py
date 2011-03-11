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
    processor = None
    
    messageQueue = []
    deadPeerQueue = []
    stabQueue = [] # The queue to store stablizing messages from other nodes
    
    b = 2
    l = 8
    localHost = "0.0.0.0"
    listenPort = 10086
    sendPort = 10086
    port = 10086

    msgLock = None
    deadPeerLock = None
    tableLock = None
    stabLock = None
    


    def __init__(self, pId, host = None, joinPort = 10086):
        self.pId = pId
        self.localHost = self.getLocalHost()

        self.comm = Communicator(self)

        for i in xrange(self.l):
            self.routeTable[i] = [None, None]
        self.routeMappingTable[None] = None

        print "A peer with pId: " + str(pId) + " is created!"

        self.msgLock = threading.Lock()
        self.deadPeerLock = threading.Lock()
        self.tableLock = threading.Lock()
        self.stabLock = threading.Lock()

        # self.processor = Processor(self)
        # self.processor.setDaemon(True)
        # self.processor.start()

        # if host != None:
        #     print "join called!"
        #     self.join(host, joinPort)
        
        self.localOperation()        

    def setPId(self, pId):
        if pId >= 0 and pId<pow(2, self.l):
            self.pId = pId;
        else:
            print "invalid peer id, default to -1..."
            self.pId = -1;

    def getLocalHost(self):
        return socket.gethostname()

    def getMessage(self): #return the first message in the message queue, and delete it.
        # check lock first
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
        print "joining sending\t", self.send(hostIP, port, "join" + "\t" + "0" + "\t" + str(self.localHost) + "\t" + str(self.pId))

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


        
    def stablize(self, peerId):
        if peerId in self.leafTable:
            del self.leafTable[peerId]
        if peerId in self.neighborTable:
            del self.neighborTable[peerId]
        if peerId in self.routeMappingTable:
            del self.routeMappingTable[peerId]

            cmsb = getCommonMSB(peerId, self.pId, self.l)
            lBit = int(getBitStringToLength(peerId, self.l)[len(cmsb)])
            self.routeTable[len(cmsb)][lBit] = None


    
    def terminate(self):
        print str(self.pId) + " is terminating ..."

    def leave(self):
        print str(self.pId) + " has left the system."

    def stablize(self):
        print "stablize"

    def route(self, key):
        key = int(key)
        if key == self.pId:
            return "find", self.pId, self.localHost
        
        if len(self.leafTable)!=0 and abs(key-self.pId) < pow(2, self.b):
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
        print "sending ..."

        # socket setting 
        buf = 1024 * 1024
        addr = (host, sendPort)
        
        family, socktype, proto, garbage, address = socket.getaddrinfo(host, sendPort)[0]
        # sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock = socket.socket(family, socktype, proto)
        try:
            print "addr: ", addr
            sock.connect(address)
        except Exception:
            print "Exception: ", Exception
            return False
        print "overTry?"
        sock.send(content)
        sock.close()

        return True
    

    
    def printMessage(self):
        for item in self.messageQueue:
            print "message: ", item
    def prtMsgQueue(self):
        self.comm.prtMsgQueue()

    def localOperation(self):
        print "*** Welcome to peer " + str(self.pId) + '! ***'
        while (True):
            print '''Please select operation:
            \t'l') volunteerly leave the network;
            \t't') terminate without telling anyone;
            \t'j') Join;
            \t'send') send a message;
            \t'prt') print all messages;
            \t'next') get next message, and delete it from the messageQueue;
            \t'r') search a key
            \t'pT' print tables
            \t'q') logout peer.'''

            option = raw_input("Your command: ")
            if option == 'l':
                self.leave()
                return
            elif option == 't':
                self.terminate()
                return
            elif option == 'send':
                host = raw_input("Please input the objective hostname: ")
                message = raw_input("Please input the message: ")
                result = self.send(host, self.sendPort, message)
                if result == True:
                    print "Message has been sent."
                else:
                    print "Message sending failed."
            elif option == 'prt':
                self.printMessage()
            elif option == 'next':
                print self.getMessage()
            elif option == 'j':
                #host = raw_input("Please input hostname:")
                host = "bug06.umiacs.umd.edu"
                self.join(host, 10086)
            elif option == 'r':
                key = raw_input("Please input the key to query:")
                print self.route(key)
            elif option == 'pT':
                print "leafTable:"
                print self.leafTable
                print "routeTable:"
                print self.routeTable
                print "routeMappingTable: "
                print self.routeMappingTable
                print "neighborTable:"                
                print self.neighborTable
            elif option == 'q':
                return
            else:
                print "Please choose an option:"
                continue

class Listener(threading.Thread):
    # def __init__(self, host, port, messageQueue):
    def __init__(self, peer):
        threading.Thread.__init__(self, name = "noname")
        print "A listener is created!"
        self.host = ""
        self.listenPort = peer.listenPort
        self.messageQueue = peer.messageQueue
        self.peer = peer

    def run(self):
        # listen to the host, and write everything into messageQueue

        addr = ("",self.listenPort)
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.bind(addr)
        buf = 1024 * 1024
        sock.listen(100)
        
        while True:
            print "Establishing connection..."
            conn, addr2 = sock.accept()
            print "Connection established!"
            # data,addr2 = sock.recvfrom(buf)
            data = conn.recv(buf)

            print "data received!", data

            if not data:
                print "Why no data coming?"
                exit()
            else:
                # Preprocess message from others
                category = data.split('\t')[0]
                if category == "stable": # it's a stablizing message, to stabQueue
                    if self.peer.stabLock.acquire():
                        self.peer.stabQueue.append(data)
                        self.peer.stabLock.release()
                # check msgLock()
                else:
                    if self.peer.msgLock.acquire():
                        self.messageQueue.append(data)
                        self.peer.msgLock.release()
            time.sleep(0.5)


            
class Communicator:
    messageQueue = None 
    def __init__(self, peer):
        print "A communicator is created!"
        self.messageQueue = peer.messageQueue
        self.peer = peer
        self.mylistener = Listener(peer)
        self.mylistener.setDaemon(True)
        self.mylistener.start()
        # self.operation()
        
    def send(self, host, sendPort, content):
        print "sending ..."

        # socket setting 
        buf = 1024 * 1024
        addr = (host, sendPort)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            print "addr: ", addr
            print "error", sock.connect_ex(addr)
        except Exception:
            print "Exception: ", Exception
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
    #             self.send(host, 10086, content)
    #         elif option == 'prt':
    #             print self.messageQueue
    #         elif option == 'q':
    #             break
    #         else:
    #             print "Please choose an option:"
    #             continue
            


class Processor(threading.Thread):
    def __init__(self, peer):
        threading.Thread.__init__(self, name = "processor")
        print "a processor is created..."
        self.peer = peer;
        
    def run(self):
        while True:
            msg = self.peer.getMessage()
            if msg != None:
                print "receive message: ", msg
                token = msg.split("\t")
                if token[0]=="join":
                    if len(token)!=4:
                        print "invalid join message format..."
                        continue
                    
                    joinLevel = int(token[1])
                    if joinLevel<0 or joinLevel>=self.peer.l:
                        print "invalid join level, something wrong with routing..."
                        continue
                    
                    hostIP = token[2]
                    hostID = int(token[3])

                    print joinLevel, "\t", hostIP, "\t", hostID
                    
                    if joinLevel==0:
                        print "first send status"
                        print hostIP, self.peer.port, "table" + "\t" + str(self.peer.localHost) + "\t" + str(self.peer.pId) + "\t" + self.peer.serialize("neighbor")
                        print self.peer.send(hostIP, self.peer.port, "table" + "\t" + str(self.peer.localHost) + "\t" + str(self.peer.pId) + "\t" + self.peer.serialize("neighbor"))
                        
                    print "second send status"
                    print self.peer.send(hostIP, self.peer.port, "table" + "\t" + str(self.peer.localHost) + "\t" + str(self.peer.pId) + "\t" + self.peer.serialize("route", joinLevel))
                    
                    while True:
                        status, pid, pip = self.peer.route(hostID)

                        print status, "\t", pid, "\t", pip

                        if status=="find":
                            print "find send status:"
                            print self.peer.send(hostIP, self.peer.port, "find" + str(joinLevel) + "\t" + str(pip) + "\t" + str(pid))
                            print self.peer.send(hostIP, self.peer.port, "table" + "\t" + str(pip) + "\t" + str(pid) + "\t" + self.peer.serialize("leaf"))
                            print "send over"
                            break;
                        elif status=="contact":
                            if self.peer.send(pip, self.peer.port, "join" + str(joinLevel+1) + "\t" + str(hostIP) + "\t" + str(hostID)):
                                break;
                            else:
                                # TODO: get lock, call stablize
                                if self.peer.tableLock.acquire():
                                    print "detected failed peer in route table..."
                                    self.peer.stablize(pid)
                                    self.peer.tableLock.release()
                            
                    if self.peer.tableLock.acquire():
                        self.peer.addNewNode(hostIP, hostID)
                        
                elif token[0]=="route":
                    print "route"
                elif token[0]=="acquire":
                    if len(token)!=4:
                        print "invalid acquire message format..."
                        continue
                    
                    hostIP = token[1]
                    hostID = int(token[2])
                    tableType = token[3]
                    
                    #TODO: serialize table
                    
                elif token[0]=="table":
                    hostIP = token[1]
                    hostID = int(token[2])
                    
                    # TODO: get locks
                    tableType, tableContent = self.peer.deserialize("\t".join(token[3:]));
                    
                    if self.peer.tableLock.acquire():
                        if str(tableType).isdigit():
                            self.peer.routeTable[tableType] = [int(tableContent[1]), int(tableContent[2])]
                            self.peer.routeMappingTable[int(tableContent[1])] = tableContent[3]
                            self.peer.routeMappingTable[int(tableContent[2])] = tableContent[4]
                        elif tableType=="leaf":
                            self.peer.leafTable = tableContent
                        elif tableType=="neighbor":
                            self.peer.neighborTable = tableContent
                            
                        self.peer.tableLock.release()
                elif token[0]=="find":
                    print ""
                elif token[0]=="live":
                    print ""



##############################
# Main
##############################            

if len(sys.argv) == 1:
    print "This is the first peer of the system."
    host = None
    joinPort = 0
elif len(sys.argv) == 3:
    host = sys.argv[1]
    joinPort = sys.argv[2]
    print "Connecting via peer on " + host + ":" + str(joinPort) + "."
else:
    print "Wrong argument list."
    sys.exit(1)
pId = random.randint(0,255)
selfPeer = Peer(pId, host, joinPort)
