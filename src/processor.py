import threading

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
                            print tableContent
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
