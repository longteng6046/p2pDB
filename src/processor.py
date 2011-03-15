import threading
import time

class Processor(threading.Thread):
    def __init__(self, peer):
        threading.Thread.__init__(self, name = "processor")
        # print "a processor is created..."
        self.peer = peer;
        self.flag = True
        
    def run(self):
        while self.flag == True:
            if self.peer.terminateFlag == True:
                break
            
            msg = self.peer.getMessage()
            if msg != None:
                # print "receive message: ", msg
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
                    
                    if self.peer.tableLock.acquire():
                        print "\nProcessing join requrest from " + hostIP
                        if joinLevel==0:
                            self.peer.send(hostIP, self.peer.port, "table" + "\t" + str(self.peer.localHost) + "\t" + str(self.peer.pId) + "\t" + self.peer.serialize("neighbor"))
                        
                        self.peer.send(hostIP, self.peer.port, "table" + "\t" + str(self.peer.localHost) + "\t" + str(self.peer.pId) + "\t" + self.peer.serialize("route", joinLevel))

                        self.peer.tableLock.release()
                    
                    while self.flag == True:
                        if self.peer.tableLock.acquire():
                            # route the node id of the new joined node
                            status, pid, pip = self.peer.route(hostID)

                            if status=="find":
                                # if the destination is successfully found by route function
    
                                # tell the destination node to send its leaf table to the new joined node
                                self.peer.send(hostIP, self.peer.port, "table" + "\t" + str(pip) + "\t" + str(pid) + "\t" + self.peer.serialize("leaf"))

                                # tell the new node that route destination is found
                                self.peer.send(hostIP, self.peer.port, "find" + "\t" + str(joinLevel) + "\t" + str(pip) + "\t" + str(pid))
                                
                                self.peer.tableLock.release()
                                break;

                            elif status=="contact":
                                # if the place returned from route function is an intermediate node
                                # tell the intermediate node that a new node is joining
                                if self.peer.send(pip, self.peer.port, "join" + "\t" + str(joinLevel+1) + "\t" + str(hostIP) + "\t" + str(hostID)):
                                    self.peer.tableLock.release()
                                    break;
                                else:
                                    # print("detected failed peer with id %s and ip address %s in route table...", %(pid, pip))
                                    self.peer.stablize(pid)

                            self.peer.tableLock.release()

                    if self.peer.tableLock.acquire():
                        self.peer.addNewNode(hostIP, hostID)
                        self.peer.tableLock.release()

                elif token[0]=="find":
                    if len(token)!=4:
                        print "invalid find message format..."
                        continue
                    
                    joinLevel = int(token[1])
                    if joinLevel<0 or joinLevel>=self.peer.l:
                        print "invalid find level, something wrong with routing..."
                        continue
                    
                    hostIP = token[2]
                    hostID = int(token[3])

                    if hostID==self.peer.pId:
                        # collision on peer ID space
                        print "collision on peer ID space..."
                    
                    if self.peer.tableLock.acquire():
                        self.peer.addNewNode(hostIP, hostID)
                        self.peer.tableLock.release()
                        
                elif token[0]=="route":
                    if len(token)!=5:
                        print "invalid route message format..."
                        continue
                    
                    hopCount = int(token[1])
                    hostIP = token[2]
                    hostID = int(token[3])
                    key = token[4]
                   
                    while self.flag == True:
                        if self.peer.tableLock.acquire():
                            routeMsg, pid, pip = self.route(key)
        
                            if routeMsg=="find":
                                print "\nFind the host of the key on: ", pip
                                self.peer.send(hostIP, self.peer.port, "return" + "\t" + str(hopCount) + "\t" + str(pip) + "\t" + str(pid) + "\t" + key)
                                #print "closest node is current node with id " + pid + " and ip " + pip + "..."
                                break;
                            elif routeMsg=="contact":
                                print "\nPassing routing request to node: ", pip
                                #print "relay to node with id " + pid + " and ip " + pip + "..."
                                if self.peer.send(pip, self.peer.port, "route" + "\t" + str(hopCount+1) + "\t" + str(hostIP) + "\t" + str(hostID) + "\t" + key):
                                    self.peer.tableLock.release()
                                    break
                                else:
                                    # print("detected failed peer with id %s and ip address %s in route table...", %(pid, pip))
                                    self.peer.stablize(pid)
                                
                            self.peer.tableLock.release()

                elif token[0]=="return":
                    if len(token)!=5:
                        print "invalid return message format..."
                        continue

                    hopCount = int(token[1])
                    destinationIP = token[2]
                    destinationID = int(token[3])
                    key = token[4]

                    print "route key " + key + " to node with ip " + destinationIP + " and id " + str(destinationID) + " in " + str(hopCount) + " hops..."

                elif token[0]=="acquire":
                    if len(token)!=4:
                        print "invalid acquire message format..."
                        continue
                    
                    hostIP = token[1]
                    hostID = int(token[2])
                    tableType = token[3]

                    if self.peer.tableLock.acquire():
                        # serialize the required table and send to desired ip
                        self.peer.send(hostIP, self.peer.port, "stable" + "\t" + str(self.peer.localHost) + "\t" + str(self.peer.pId) + "\t" + self.peer.serialize(tableType))
                        self.peer.tableLock.release()
                    
                elif token[0]=="table":
                    hostIP = token[1]
                    hostID = int(token[2])
                    
                    tableType, tableContent = self.peer.deserialize("\t".join(token[3:]));
                    if self.peer.tableLock.acquire():
                        #print tableType, tableContent
                        
                        if str(tableType).isdigit():
                            for index in xrange(4):
                                if tableContent[index]=="None":
                                    tableContent[index] = None
                                else:
                                    if index==0 or index==2:
                                        tableContent[index] = int(tableContent[index])



                            self.peer.routeTable[tableType] = [tableContent[0], tableContent[2]]
                            self.peer.routeMappingTable[tableContent[0]] = tableContent[1]
                            self.peer.routeMappingTable[tableContent[2]] = tableContent[3]
                        elif tableType=="leaf":
                            self.peer.leafTable = tableContent
                        elif tableType=="neighbor":
                            self.peer.neighborTable = tableContent
                            
                        self.peer.tableLock.release()

                time.sleep(1)

        return

    def kill(self):
        self.flag = False
        
