import threading
import socket

class Listener(threading.Thread):
    def __init__(self, host, port, messageQueue):
        threading.Thread.__init__(self, name = "noname")
        print "a Listener is created..."
        self.host = host
        self.port = port
        self.messageQueue = messageQueue
        
    def run(self):
        # listen to the host, and write everything into messageQueue

        addr = (self.host,self.port)
        sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        sock.bind(addr)
        buf = 1024

        while 1:
            data,addr2 = sock.recvfrom(buf)
            if not data:
                print "Why no data coming?"
                exit()
            else:
                self.messageQueue.append((addr2, data))
                # print "message from: "
                # print addr2
                # print data