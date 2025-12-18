import socket, sys
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(('0.0.0.0', 9999))
print('UDP_READY', flush=True)
while True:
    data, addr = sock.recvfrom(1024)
    sock.sendto(b'PONG_THREAD', addr)
