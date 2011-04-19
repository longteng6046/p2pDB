import threading
import socket
import time

from sqlChannel import *

class SqlMsgListener(threading.Thread):
    port = -1
    msgStack = None
    localDaemon = None

    def __init__(self, listenPort, msgStack, daemon):
        threading.Thread.__init__(self, name = "sqlMsgListener")

        print "testmsg: creating an SqlMsgListener."
        self.port = listenPort
        self.msgStack = msgStack
        self.localDaemon = daemon

        

    def getAPort(self):
        return self.localDaemon.getCurrentSqlPort()
    def run(self):
        # print "port: ", self.port
        s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        print "listener binding with: ", self.port
        s.bind(('', self.port))
        s.listen(100)
        # print "lister listening to: ", self.port
        while True:
            con,addr = s.accept()
            # print "testmsg: connect from", addr
            msg = con.recv(1024 * 1024)
            if not msg:
                break

            print "msg got in the listener: ", msg, "from:", self.port
            localPort = self.getAPort()

            
            channel = SqlChannel(addr[0], int(msg), localPort, "inSqlMsgListener", False)
            channel.setDaemon(True)
            channel.start()
            time.sleep(1)
            channel.confirm()
            

            
            
        print "closing sqlMsgListener..."
        con.close()

    def stop(self):
        self._stop.set()
