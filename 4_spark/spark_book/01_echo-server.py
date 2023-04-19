# echo-server.py
# server to act like 'nc -l 65431' in apache structured streaming

import socket
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("port")
args = parser.parse_args()


HOST = "127.0.0.1"  # Standard loopback interface address (localhost)
PORT = int(args.port)  # Port to listen on (non-privileged ports are > 1023)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    conn, addr = s.accept()
    print('Client Connected')
    with conn:
        print(f"Connected by {addr}")
        while True:
            data = input() or conn.recv(1024).decode()
            data+="\n"
            if not data:
                break
            conn.sendall(data.encode())

