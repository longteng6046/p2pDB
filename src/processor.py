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
                    
                    if joinLevel==0:
                        self.peer.send(hostIP, self.peer.port, "table" + "\t" + str(self.peer.localHost) + "\t" + str(self.peer.pId) + "\t" + self.peer.serialize("neighbor"))
                        
                    self.peer.send(hostIP, self.peer.port, "table" + "\t" + str(self.peer.localHost) + "\t" + str(self.peer.pId) + "\t" + self.peer.serialize("route", joinLevel))
                    
                    while True:
                        status, pid, pip = self.peer.route(hostID)
                        
                        if status=="find":
                            self.peer.send(hostIP, self.peer.port, "find" + "\t" + str(joinLevel) + "\t" + str(pip) + "\t" + str(pid))
                            self.peer.send(hostIP, self.peer.port, "table" + "\t" + str(pip) + "\t" + str(pid) + "\t" + self.peer.serialize("leaf"))
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
                        self.peer.tableLock.release()

                elif token[0]=="find":
                    if len(token)!=4:
                        print "invalid find message format..."
                        continue
                    
                    joinLevel = int(token[1])
                    if joinLevel<0 or joinLevel>=self.peer.l:
                        print "invalid join level, something wrong with routing..."
                        continue
                    
                    hostIP = token[2]
                    hostID = int(token[3])
                    
                    if self.peer.tableLock.acquire():
                        self.peer.addNewNode(hostIP, hostID)
                        self.peer.tableLock.release()
                        
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
                        print tableType, tableContent
                        
                        if str(tableType).isdigit():
                            for index in xrange(4):
                                if tableContent[index]=="None":
                                    tableContent[index] = None
                                else:
                                    tableContent[index] = int(tableContent[index])

                            self.peer.routeTable[tableType] = [tableContent[0], tableContent[2]]
                            self.peer.routeMappingTable[tableContent[0]] = tableContent[1]
                            self.peer.routeMappingTable[tableContent[2]] = tableContent[3]
                        elif tableType=="leaf":
                            self.peer.leafTable = tableContent
                        elif tableType=="neighbor":
                            self.peer.neighborTable = tableContent
                            
                        self.peer.tableLock.release()

                elif token[0]=="live":
                    print ""
