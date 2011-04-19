import threading
import time
import sys

from copy import deepcopy

class LiveChecker(threading.Thread):
    def __init__(self, peer):
        threading.Thread.__init__(self, name = "noname")
        
        self.peer = peer
        self.pID = self.peer.getID()
        self.pIP = self.peer.getIP()
        self.leafLength = self.peer.leafRange
        self.neighborLength = self.peer.neighborRange
        self.length = self.peer.length
        self.port = self.peer.port
        self.base_power = self.peer.base_power
        
        self.flag = True
        
    def run(self):
        from id_ops import findClosestID, findFurthestID
        from ip_ops import findClosestIP, findFurthestIP
        
        # scan neighbor, see who is dead.
        while self.flag == True:
            if self.peer.terminateFlag == True:
                break
            
            time.sleep(1) # sleep for 10 seconds

            if self.peer.tableLock.acquire():
                # Check whether nodes in leaf/neighbor are still alive
                nTable = deepcopy(self.peer.neighborTable)
                for nodeId in nTable:
                    nodeHost = nTable[nodeId]
                    print "send 1........"
                    result = self.peer.send(nodeHost, self.port, "live")
                    if result == False:
                        # print "peer neighbor table before: ", self.peer.neighborTable
                        self.peer.stablize(nodeId)
                        # print "peer neighbor table after: ", self.peer.neighborTable
                lTable = deepcopy(self.peer.leafTable)
                for nodeId in lTable:
                    nodeHost = lTable[nodeId] 
                    print "send 2........"
                    result = self.peer.send(nodeHost, self.port, "live")
                    if result == False:
                        self.peer.stablize(nodeId)

                # Check whether the two tables are full. If not, acquire table from others.
                self.peer.tableLock.release()

            time.sleep(1)
            if self.peer.tableLock.acquire():
                if len(self.peer.neighborTable) < self.neighborLength:
                    farNode = findFurthestID(self.pID, self.peer.neighborTable)
                    if farNode != None:
                        objId, objHost = farNode
                        # print "objHost: ", objHost
                        msg = "acquire\t" + self.peer.getLocalHost() + "\t" + str(self.peer.pId) + "\tneighbor"
                        print "send 3........"
                        self.peer.send(objHost, self.port, msg)

                if len(self.peer.leafTable) < self.leafLength:
                    farNode = findFurthestIP(self.pIP, self.peer.neighborTable)
                    if farNode != None:
                        # print "objHost: ", objHost                        
                        objId, objHost = farNode
                        msg = "acquire\t" + self.peer.getLocalHost() + "\t" + str(self.peer.pId) + "\tleaf"
                        print "send 4........"
                        self.peer.send(objHost, self.port, msg)
                self.peer.tableLock.release()

            time.sleep(1)
            # then process feedbacks from oher nodes to repopulize
            # leaftable and neighbor table
                
            if self.peer.stabLock.acquire():
                if len(self.peer.stabQueue) == 0:  # no feedback in the queue
                    self.peer.stabLock.release()
                    continue
                else:
                    tmpQueue = deepcopy(self.peer.stabQueue)
                    self.peer.stabQueue = []
                    for msg in tmpQueue:
                        tablePartSerial = "\t".join(msg.split('\t')[3:]) #gets the neighbor/leaf table we asked

                        # need to get the table in the way of dictionaries
                        tableType, nodesPair  = self.peer.deserialize(tablePartSerial)

                        if tableType == "leaf": # stablize leafTable
                            if self.peer.tableLock.acquire():
                            # Check whether the leaf is having a vacanci
                                if len(self.peer.leafTable) < self.leafLength:
                                    numVacc = self.leafLength - len(self.peer.leafTable)
                                # Add stuf into leafTable
                                    leafAddList = [] # a list of (key, val) pairs, not dict
                                    for i in range(0,numVacc):
                                        pair = findCloestID(self.pID, nodesPair) 
                                        if pair != None and pair[0] not in self.peer.leafTable:
                                            leafAddList.append(pair)
                                    if len(leafAddList) != 0:
                                        for item in leafAddList:
                                            self.peer.leafTable[ item[0] ] = item[1]
                                self.peer.tableLock.release()

                        elif tableType == "neighbor":
                            # check vacancy of neighbor table
                            if len(nodesPair) < self.neighborLength:
                                if self.peer.tableLock.acquire():
                                    numVacc = self.neighborLength - len(nodesPair)
                                # Add nodes to neighbor table
                                    neighborAddList = [] # a list of (key, val) pairs
                                
                                    for i in range(0, numVacc):
                                        pair = self.retrieveCloseNeighbor(nodesPair) # ***************************
                                        if pair != None and pair[0] not in self.peer.neighborTable:
                                            neighborAddList.append(pair)
                                    if len(neighborAddList) != 0:

                                        for item in neighborAddList:
                                            self.peer.neighborTable[ item[0] ] = item[1]
                                    self.peer.tableLock.release()
                        else:
                            print "Error! Wired message  send to lifeChecker"
                            exit(1)

                self.peer.stabLock.release()

        return
        
    def kill(self):
        self.flag = False
