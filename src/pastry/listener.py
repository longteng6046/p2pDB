import threading
import sys
import socket

class Listener(threading.Thread):
    # def __init__(self, host, port, messageQueue):
    def __init__(self, peer):
        super(Listener, self).__init__()
        self._stop = threading.Event()
        # threading.Thread.__init__(self, name = "noname")
        # print "A listener is created!"
        
        self.peer = peer
        self.port = self.peer.port
        self.messageQueue = self.peer.messageQueue
        self.flag = True

    def stop(self):
        self._stop.set()

    def run(self):
        # listen to the host, and write everything into messageQueue
        while self.flag == True:
            addr = ("",self.port)
            sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            sock.bind(addr)
            buf = 1024 * 1024
            sock.listen(10)

            while self.flag == True:
                # print "Establishing connection..."
                try:
                    conn, addr2 = sock.accept()
                except:
                    print "exception in listener. lost node"
                    print "addr2: ", addr2
                    losthost = addr2[0]
                    if self.findId(losthost) != None:
                        self.stablize(self.findId(losthost))
                    break
            # print "Connection established!"
            # data,addr2 = sock.recvfrom(buf)
                data = conn.recv(buf)
            
            # print "data received!", data

                if not data:
                    break
                else:
                    # Preprocess message from others
                    category = data.split('\t')[0]
                    if category == "stable": # it's a stablizing message, to stabQueue
                        if self.peer.stabLock.acquire():
                            self.peer.stabQueue.append(data)
                            self.peer.stabLock.release()
                # check msgLock()
                    elif category == "live":
                        continue
                    else:
                        if self.peer.msgLock.acquire():
                            self.messageQueue.append(data)
                            self.peer.msgLock.release()

            time.sleep(1)

        return
                            
    def kill(self):
        self.flag = False