import threading
import socket

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