import socket, sys, time
print('Starting UDP Service...', flush=True)
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(('0.0.0.0', 9999))
print('UDP_READY', flush=True)
while True:
    try:
        data, addr = sock.recvfrom(1024)
        print(f'--> Received Packet: {str(data)}', flush=True)
        sock.sendto(b'PONG_DEPLOYED', addr)
    except Exception as e:
        print(f'Error: {e}', flush=True)
