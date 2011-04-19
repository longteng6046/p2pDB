# balabala

import socket
import threading

class SqlChannel(threading.Thread):
    objHost = None
    objPort = None
    dataObjPort = None
    localPort = None
    dataConn = None
    readyFlag = False # is it ready for data transfer?
    name = ""
    initFlag = True

    
    # send localPort to objHost:objPort, request a TCP conn. 
    def __init__(self, objHost, objPort, localPort, name, initFlag):
        threading.Thread.__init__(self, name = "sqlChannel")

        self.objHost = objHost
        self.objPort = objPort
        self.localPort = localPort
        self.name = name
        # whether this channel is initializing the connection, so it
        # needs to wait for the confirm?
        self.initFlag = initFlag
        self._stopEvent = threading.Event()        
    def connect(self):
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        # print "connecting to:", (self.objHost, self.objPort)
        sock.connect((self.objHost, self.objPort))
        sock.send(str(self.localPort))
        sock.close()

    def executeSQL(self, msg):
        if msg.split('***I am the spliting line***')[0] == 'sql':
            return "\nThis is supposed to be the sql execution result."
        else:
            return "Please check your sql format."
        
            
    def confirm(self):
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        # print "connect in confirm:", (self.objHost, self.objPort)

        
        self.dataObjPort = self.objPort
        self.readyFlag = True

        sock.connect((self.objHost, self.objPort))
        sock.send(str(self.localPort))
        sock.close()

    def run(self):
        # print self.name, "running..."
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        print "name: ", self.name, "listen to:", (self.objHost, self.localPort)
        # sock.bind((self.objHost, self.localPort))
        sock.bind(("", self.localPort))
        
        sock.listen(10)

        # while self.readyFlag == False:
        if self.initFlag == True:
            # print "waiting for confirm at:", 
            conn, addr = sock.accept()

            msg = conn.recv(1024 * 1024)
            if not msg:
                return
            # print "confirm msg got from", addr            
            # print "confirm msg:", msg

            self.dataObjPort = int(msg)

            self.readyFlag = True
            # print "dataObjPort in", self.name, self.dataObjPort
            # print "name", self.name
            # print "ready flag:", self.readyFlag
            
        # print "going on..."
        while not self._stopEvent.isSet():
            # print "name:", self.name, " in data mode, watching:", (self.objHost, self.localPort)
            conn, addr = sock.accept()
            # print "data got from", addr

            msg = conn.recv(1024 * 1024)
            if not msg:
                break


            print "data received in", self.name, "msg: ", msg

            if self.initFlag == False:
                resultData = self.executeSQL(msg)
                self.send(resultData)

        print self.name, 'terminated.'
        
    def send(self, content):
        # print "dataObjPort in", self.name, self.dataObjPort
        content = 'sql***I am the spliting line***' + content
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.connect((self.objHost, self.dataObjPort))

        sock.send(content)
        sock.close()
        
    def exit(self):
        self._stopEvent.set()
        threading.Thread.join(self)

        
