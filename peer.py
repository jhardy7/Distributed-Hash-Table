import socket
import threading
import csv
import time

id = None
ringSize = None
neighbor = None
hashTable = None

def isPrime(n):
    if n < 2: return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0: return False
    return True

def getTableSize(l):
    s = 2 * l + 1
    while not isPrime(s):
        s += 1
    return s

def populateTable(filename, s, size):
    global id,hashTable, neighbor

    with open(filename, 'r') as f:
        reader = csv.reader(f)
        next(reader)

        for row in reader:
            eventId = int(row[0])
            pos = eventId % s
            targetId = pos % int(size)

            if targetId == id:
                hashTable[pos] = row
            else:
                payload = "|".join(row)
                command = f"store {targetId} {pos} {payload}"
                mSocket.sendto(command.encode(),('localhost', int(neighbor.split(",")[2])))
                time.sleep(0.001)

def betweenPeer(pPort):
    global id, ringSize, neighbor, hashTable

    pSocket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    pSocket.bind(('localhost',int(pPort)))
    while True:
        command, address = pSocket.recvfrom(8192)
        ogCommand = command
        command = command.decode().split()

        match command:
            case ["set-id", ids, ringSizes, neighbors]:
                id = ids
                ringSize = ringSizes
                neighbor = neighbors
            
            case ["build-hash", s]:
                hashTable = [None] * int(s)

            case ["store", targetId, pos, payload]:
                targetId = int(targetId)
                pos = int(pos)

                if targetId == id:
                    hashTable[pos] = payload.split("|")
                else:
                    pSocket.sendto(ogCommand, ('localhost', int(neighbor.split(",")[2])))

mSocket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)

while True:
    command = input()
    mSocket.sendto(command.encode(),('localhost',3500))

    if command == "exit":
        break

    message, address = mSocket.recvfrom(1024)
    message = message.decode().split()

    match message:
        case ["SUCCESS", "register", pPort]:
            print(message[0])
            threading.Thread(target=betweenPeer,args=(pPort,),daemon=True).start()

        case ["SUCCESS", "setup-dht", year, *peers]:
            print(message[0], message[1], peers)

            id = 0
            ringSize = len(peers)
            neighbor = peers[1 % ringSize]

            size = len(peers)
            for i in range(size):
                pPort = peers[i].split(",")[2]
                command = f"set-id {i} {size} {peers[(i+1)% size]}"
                mSocket.sendto(command.encode(),('localhost',int(pPort)))  

            filename = f"data/StormEvents_details-ftp_v1.0_d{year}_c20250520.csv"
            with open(filename, 'r') as file: 
                lines = sum(1 for line in file) - 1    
                
            s = getTableSize(lines)
            for i in range(size):
                pPort = peers[i].split(",")[2]
                command = f"build-hash {s}"
                mSocket.sendto(command.encode(),('localhost',int(pPort))) 
            populateTable(filename,s,size)

            command = f"dht-complete {peers[0].split(","[0])}"
            mSocket.sendto(command.encode(),('localhost',3500))

        case _:
            print(message[0])

mSocket.close()