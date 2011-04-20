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
                    (objId, objHost) = findFurthestID(self.pID, self.peer.neighborTable)
                    if objId != None and objHost != None:
                        msg = "acquire\t" + self.pIP + "\t" + str(self.pID) + "\tneighbor"
                        self.peer.send(objHost, self.port, msg)

                if len(self.peer.leafTable) < self.leafLength:
                    (objId, objHost) = findFurthestIP(self.pIP, self.peer.neighborTable)
                    if objId!=None and objHost!=None:
                        msg = "acquire\t" + self.pIP + "\t" + str(self.pID) + "\tleaf"
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
                                        (objId, objHost) = findCloestID(self.pID, nodesPair) 
                                        if objId != None and objHost != None and objId not in self.peer.leafTable:
                                            leafAddList.append((objId, objHost))
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
                                        (objId, objHost) = self.retrieveCloseNeighbor(nodesPair) # ***************************
                                        if objId != None and objHost != None and objId not in self.peer.neighborTable:
                                            neighborAddList.append((objId, objHost))
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
