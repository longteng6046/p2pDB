class Communicator:
    messageQueue = []    
    def __init__(self, peer_list, pid_list):
        print "A communicator is created!"

    def send(self, sourceId, destId, content):
        tuple = (sourceId, destId, content)
        self.messageQueue.append(tuple)

    def rcv(self, myPId):
        msgList = []
        newMsgQueue = []
        for item in self.messageQueue:
            if item[1] == myPId:
                msgList.append(item)
            else:
                newMsgQueue.append(item)
        self.messageQueue = newMsgQueue
        return msgList

    def prtMsgQueue(self):
        for item in self.messageQueue:
            print item