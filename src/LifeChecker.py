import threading
import time
import sys
from copy import deepcopy

class LifeChecker(threading.Thread):
    leafRange = 32
    
    def __init__(self, peer):
        threading.Thread.__init__(self, name = "noname")
        self.peer = peer
        print "A LifeChecker thread is created!"

        self.flag = True
        
    def run(self):
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
                    result = self.peer.send(nodeHost, self.peer.sendPort, "live")
                    if result == False:
                        # print "peer neighbor table before: ", self.peer.neighborTable
                        self.peer.stablize(nodeId)
                        # print "peer neighbor table after: ", self.peer.neighborTable
                lTable = deepcopy(self.peer.leafTable)
                for nodeId in lTable:
                    nodeHost = lTable[nodeId] 
                    print "send 2........"
                    result = self.peer.send(nodeHost, self.peer.sendPort, "live")
                    if result == False:
                        self.peer.stablize(nodeId)

                # Check whether the two tables are full. If not, acquire table from others.
                tableLength = pow(2, self.peer.b + 1)
                self.peer.tableLock.release()

            time.sleep(1)
            if self.peer.tableLock.acquire():
                if len(self.peer.neighborTable) < tableLength:
                    farNode = self.retriveFarLeaf(self.peer.neighborTable)
                    if farNode != None:
                        objId, objHost = farNode
                        # print "objHost: ", objHost
                        msg = "acquire\t" + self.peer.getLocalHost() + "\t" + str(self.peer.pId) + "\tneighbor"
                        print "send 3........"
                        self.peer.send(objHost, self.peer.sendPort, msg)


                if len(self.peer.leafTable) < tableLength:
                    farNode = self.retrieveFarNeighbor(self.peer.neighborTable)
                    if farNode != None:
                        # print "objHost: ", objHost                        
                        objId, objHost = farNode
                        msg = "acquire\t" + self.peer.getLocalHost() + "\t" + str(self.peer.pId) + "\tleaf"
                        print "send 4........"
                        self.peer.send(objHost, self.peer.sendPort, msg)
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
                        # print "table before serialize is ", tablePartSerial
                        tableType, nodesPair  = self.peer.deserialize(tablePartSerial)
                        # print "table after serialize is ", tableType, nodesPair
                        tableLength = pow(2, self.peer.b + 1)

                        if tableType == "leaf": # stablize leafTable
                            if self.peer.tableLock.acquire():
                            # Check whether the leaf is having a vacanci
                                if len(self.peer.leafTable) < tableLength:
                                    numVacc = tableLength - len(self.peer.leafTable)
                                # Add stuf into leafTable
                                    leafAddList = [] # a list of (key, val) pairs, not dict
                                    for i in range(0,numVacc):
                                        pair = self.retrieveCloseLeaf(nodesPair) # ******************************
                                        if pair != None and pair[0] not in self.peer.leafTable:
                                            leafAddList.append(pair)
                                    if len(leafAddList) != 0:
                                        for item in leafAddList:
                                            self.peer.leafTable[ item[0] ] = item[1]
                                self.peer.tableLock.release()

                                        
                        elif tableType == "neighbor":
                            # check vacancy of neighbor table
                            if len(nodesPair) < tableLength:
                                if self.peer.tableLock.acquire():
                                    numVacc = tableLength - len(nodesPair)
                                # Add nodes to neighbor table
                                    neighborAddList = [] # a list of (key, val) pairs

                                # print "leafTable: "
                                # print self.peer.leafTable
                                # print "neighborTable: "
                                # print self.peer.neighborTable
                                
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

            
    def retrieveCloseLeaf(self, nodesDict): # from a dict of node pairs, find ip closest
        dist = pow(self.peer.b, self.peer.l)
        pair = None

        # print "nodes dict before send 5 is ", nodesDict
        for item in nodesDict:
            if abs(item - self.peer.pId) <= dist and \
                    abs(item - self.peer.pId) <= (self.peer.leafRange / 2) and \
                    item !=self.peer.pId:
                print "send 5........"
                result = self.peer.send(nodesDict[item], self.peer.sendPort, "live")
                if result == True:
                    dist = abs(item - self.peer.pId)
                    pair = (item, nodesDict[item])
        return pair

    def retriveFarLeaf(self, nodesDict): # from a dict of node pairs, find ip farest
        dist = 0
        pair = None
        for item in nodesDict:
            if abs(item - self.peer.pId) > dist:
                dist = abs(item - self.peer.pId)
                pair = (item, nodesDict[item])
        return pair
        
    def retrieveCloseNeighbor(self, nodesDict): # from a dict of node pairs, find ip closest
        dist = 1280000
        pair = None
        localBugId = self.getBugId(self.peer.getLocalHost())
        
        # print "nodes dict before send 6 is ", nodesDict
        for item in nodesDict:
            bugId = self.getBugId(nodesDict[item])
            if abs(localBugId - bugId) <= dist and \
                    abs(localBugId - bugId) <= (self.peer.neighborRange / 2) and \
                    localBugId != bugId:
                print "send 6........"
                result = self.peer.send(nodesDict[item], self.peer.sendPort, "live")
                if result == True:
                    dist = abs(localBugId - bugId)
                    pair = (item, nodesDict[item])
        return pair

    def retrieveFarNeighbor(self, nodesDict): # from a dict of node pairs, find ip farest
        dist = 0
        pair = None
        localBugId = self.getBugId(self.peer.getLocalHost())
        for item in nodesDict:
            bugId = self.getBugId(nodesDict[item])
            if abs(localBugId - bugId) > dist:
                dist = abs(localBugId - bugId)
                pair = (item, nodesDict[item])
        return pair
    

    def getBugId(self, hostname): # get the bug id frm the bug cluster hostname
        # print "hostname: ", hostname
        return int(hostname.split('.')[0][3:])
        
    def kill(self):
        self.flag = False
