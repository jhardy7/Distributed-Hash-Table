import socket

socket = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)

while True:
    command = input()
    socket.sendto(command.encode(),('localhost',3500))

    if command == "exit":
        break

    message, address = socket.recvfrom(1024)
    print(message.decode())

socket.close()