import socket
import threading
import traceback
import csv
import time

class Peer:

    def __init__(self, name, ip, mPort, pPort):
        self.name = name
        self.ip = ip
        self.mPort = int(mPort)
        self.pPort = int(pPort)
        self.state = "free"
        self.id = None
        self.ringSize = None
        self.data = {}
        self.neighbor = None

        self.sendSocket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        self.sendSocket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 10485760)
        threading.Thread(target=self.peerListener,daemon=True).start()

    def __str__(self):
        return f"({self.name} {self.ip} {self.pPort})"

    def peerListener(self):
        try:
            self.listenSocket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
            self.listenSocket.bind(('localhost',int(self.pPort)))

            while True:
                command, address = self.listenSocket.recvfrom(8192)
                command = command.decode().split()

                match command:
                    case ["set-id", id, ringSize, *dht]:
                        self.id = int(id)
                        self.ringSize = int(ringSize)

                        if self.id == self.ringSize - 1:
                            self.neighbor = int(dht[0])
                        else:
                            self.neighbor = int(dht[self.id+1])
                            command = f"set-id {self.id + 1} {ringSize} {" ".join(map(str, dht))}"
                            self.sendSocket.sendto(command.encode(),('localhost', self.neighbor))

                    case ["store", targetId, pos, *record]:
                        if int(targetId) == self.id:
                            self.data[int(pos)] = record
                        else:
                            command = f"store {targetId} {pos} {record}"
                            self.sendSocket.sendto(command.encode(),('localhost', self.neighbor))

                    case ["find-event", eventId]:
                        for pos in self.data.keys():
                            if int(self.data[pos][7]) == eventId:
                                print(self.data[pos])
                                return
                            
                        command = f"find-event {eventId}"          
                        self.sendSocket.sendto(command.encode(), ('localhost', self.neighbor))    
        except Exception as e:
            pass

    def setupDHT(self, ringSize, dht, year):
        self.state = "leader"
        self.id = 0
        self.ringSize = int(ringSize)
        self.neighbor = dht[self.id + 1].pPort

        dhtPorts = []
        for i in range(len(dht)):
            dhtPorts.append(dht[i].pPort) 

        command = f"set-id {self.id + 1} {ringSize} {" ".join(map(str, dhtPorts))}"
        self.sendSocket.sendto(command.encode(),('localhost', self.neighbor))
        self.setData(year)

        time.sleep(2)
        command = f"dht-complete {self.name}"
        self.sendSocket.sendto(command.encode(),('localhost', 3500))

        for peer in dht:
            print(f"{peer.name}: {len(peer.data)} records")

    def setData(self, year):
        count = 0
        filename = f"./data/StormEvents_details-ftp_v1.0_d{year}_c20250520.csv"
        with open(filename, 'r') as f:
            for line in f:
                count += 1
        
        l = count - 1
        s = 2 * l + 1
        while not self.isPrime(s):
            s += 1

        with open(filename, 'r') as f:
            reader = csv.reader(f)
            next(reader)

            for row in reader:
                eventId = int(row[7]) 
                pos = eventId % s
                targetId = pos % self.ringSize
                
                if targetId == self.id:
                    self.data[pos] = row
                else:
                    record_str = "|".join(row)
                    command = f"store {targetId} {pos} {record_str}" 
                    self.sendSocket.sendto(command.encode(), ('localhost', self.neighbor))

    def query(self, eventId):
        for pos in self.data.keys():
            if int(self.data[pos][7]) == eventId:
                print(self.data[pos])
                return
            
        command = f"find-event {eventId}"          
        self.sendSocket.sendto(command.encode(), ('localhost', self.neighbor))

    def isPrime(self, n):
        if n < 2: return False
        for i in range(2, int(n**0.5) + 1):
            if n % i == 0: return False
        return True
    
    def getTableSize(self, l):
        s = 2 * l + 1
        while not self.isPrime(s):
            s += 1
        return s