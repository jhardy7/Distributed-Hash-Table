import socket
import random

def register(name, ip, mPort, pPort):
    if not name.isalpha() or len(name) > 15:
        return False

    if name in peerList:
        return False

    for peer in peerList.values():
        if peer["mPort"] == mPort or peer["pPort"] == pPort:
            return False

    peerList[name] = {"name": name,
                      "ip": ip,
                      "mPort": mPort,
                      "pPort": pPort,
                      "state": "free"}
    return True

def setup(name, size, year):
    if int(size) < 3:
        return False
    
    if len(peerList) < int(size):
        return False
    
    if dhtActive:
        return False
    
    if name not in peerList:
        return False
    
    peerList[name]["state"] = "leader"
    dht.append(peerList[name])
    
    freePeers = []
    for peer in peerList.values():
        if peer["state"] == "free":
            freePeers.append(peer)

    for x in range(int(size)-1):
        randomPeer = random.choice(freePeers)
        randomPeer["state"] = "inDHT"
        dht.append(randomPeer)
    return True

def complete(name):
    if name not in peerList:
        return False
    return peerList[name]["state"] == "leader"

socket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
socket.bind(('localhost',3500))

peerList = {}
dht = []
dhtActive = False
dhtWaitingToComplete = False

while True:
    command, address = socket.recvfrom(1024)
    command = command.decode().split()

    if dhtWaitingToComplete:
        match command:
            case ["dht-complete", name]:
                if complete(name):
                    socket.sendto(b'SUCCESS',address)
                    dhtWaitingToComplete = False
                    dhtActive = True
                else:
                    socket.sendto(b'FAILURE',address)
            case _:
                socket.sendto(b'FAILURE',address)

    else:
        match command:
            case ["register", name, ip, mPort, pPort]:
                if register(name, ip, mPort, pPort):
                    response = f"SUCCESS {command[0]} {pPort}"
                    socket.sendto(response.encode(), address)
                else:
                    response = f"FAILURE {command[0]}"
                    socket.sendto(response.encode(), address)

            case ["setup-dht", name, size, year]:
                if setup(name, size, year):
                    response = f"SUCCESS {command[0]} {year} "
                    for peer in dht:
                        response += f"{peer["name"]},{peer["ip"]},{peer["pPort"]} "
                    socket.sendto(response.encode(), address)
                    dhtWaitingToComplete = True
                else:
                    response = f"FAILURE {command[0]}"
                    socket.sendto(response.encode(), address)

            case ["dht-complete", name]:
                if complete(name):
                    response = f"SUCCESS {command[0]}"
                    socket.sendto(response.encode(), address)
                else:
                    response = f"FAILURE {command[0]}"
                    socket.sendto(response.encode(), address)

            case ["exit"]:
                break

            case _:
                socket.sendto(b'FAILURE', address)

socket.close()