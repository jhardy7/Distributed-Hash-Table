import socket
import random

class registeredPeer:
    def __init__(self, name, ip, mPort, pPort):
        self.name = name
        self.ip = ip
        self.mPort = mPort
        self.pPort = pPort
        self.state = "free"

def register(peerName, ip, mPort, pPort):
    if not peerName.isalpha() or len(peerName) > 15:
        return False
    
    if peerName in peerList:
        return False
    
    for peer in peerList.values():
        if peer.ip == ip:
            if peer.mPort == mPort or peer.pPort == pPort:
                return False

    peerList[peerName] = registeredPeer(peerName, ip, mPort, pPort)
    return True

def setup(peerName, size, year):
    if int(size) < 3:
        return False
    
    if len(peerList) < int(size):
        return False
    
    if dhtActive:
        return False
    
    if peerName not in peerList:
        return False
    
    freePeers = []

    peerList[peerName].state = "leader"
    dhtPeers.append(peerList[peerName])
    
    for peer in peerList.values():
        if peer.state == "free":
            freePeers.append(peer)

    for x in range(int(size)-1):
        randomPeer = random.choice(freePeers)
        randomPeer.state = "inDHT"
        dhtPeers.append(randomPeer)
    return True

def complete(peerName):
    if peerName not in peerList:
        return False
    return peerList[peerName].state == "leader"

def query(peerName):
    if not dhtActive:
        return False
    if peerName not in peerList:
        return False
    if peerList[peerName].state != "free":
        return False
    return True

def leave():
    pass

def join():
    pass

def rebuilt():
    pass

def deregister():
    pass

def teardown():
    pass

def teardownComplete():
    pass

peerList = {}
dhtPeers = []
dhtActive = False
dhtWaitingToComplete = False

socket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
socket.bind(('localhost',3500))

while True:
    command, address = socket.recvfrom(1024)
    command = command.decode().split()
    
    if dhtWaitingToComplete:
        match command:
            case ["dht-complete", peerName]:
                if complete(peerName):
                    socket.sendto(b'SUCCESS',address)
                    dhtWaitingToComplete = False
                    dhtActive = True
                else:
                    socket.sendto(b'FAILURE',address)
            case _:
                socket.sendto(b'FAILURE',address)
    else:
        match command:
            case ["register", peerName, ip, mPort, pPort]:
                if register(peerName, ip, mPort, pPort):
                    socket.sendto(b'SUCCESS',address)
                else:
                    socket.sendto(b'FAILURE',address)

            case ["setup-dht", peerName, size, year]:
                if setup(peerName, size, year):
                    message = "SUCCESS\n"
                    for peer in dhtPeers:
                        message += f"{peer.name} {peer.ip} {peer.pPort}\n"   
                    socket.sendto(message.encode(),address)
                    dhtWaitingToComplete = True
                else:
                    socket.sendto(b'FAILURE',address)

            case ["dht-complete", peerName]:
                if complete(peerName):
                    socket.sendto(b'SUCCESS',address)
                else:
                    socket.sendto(b'FAILURE',address)

            case ["query-dht", peerName]:
                if query(peerName):
                    socket.sendto(b'SUCCESS',address)
                else:
                    socket.sendto(b'FAILURE',address)

            case ["leave-dht", peerName]:
                socket.sendto(b'SUCCESS',address)

            case ["join-dht", peerName]:
                socket.sendto(b'SUCCESS',address)

            case ["dht-rebuilt", peerName, newLeader]:
                socket.sendto(b'SUCCESS',address)

            case ["deregister", peerName]:
                socket.sendto(b'SUCCESS',address)

            case ["teardown-dht", peerName]:
                socket.sendto(b'SUCCESS',address)

            case ["teardown-complete", peerName]:
                socket.sendto(b'SUCCESS',address)

            case ["exit"]:
                break

            case _:
                socket.sendto(b'FAILURE',address)

socket.close()