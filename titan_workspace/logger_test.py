import socket, sys
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(('0.0.0.0', 9999))
print('UDP_READY', flush=True)
while True:
    data, addr = sock.recvfrom(1024)
    # Convert bytes to string safely for logging
    print(f'--> Received Packet: {str(data)}', flush=True)
    sock.sendto(b'PONG_THREAD', addr)
