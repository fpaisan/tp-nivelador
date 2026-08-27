import socket

def recv_all(socket: socket.socket, size):
    chunks = []
    bytes_recd = 0
    while bytes_recd < size:
        chunk = socket.recv(size - bytes_recd)
        if not chunk:
            raise RuntimeError("Socket connection broken")
        chunks.append(chunk)
        bytes_recd += len(chunk)
    return b''.join(chunks)


def send_all(socket: socket.socket, bytes):
    total_sent = 0
    while total_sent < len(bytes):
        sent = socket.send(bytes[total_sent:])
        if sent == 0:
            raise RuntimeError("Socket connection broken")
        total_sent += sent