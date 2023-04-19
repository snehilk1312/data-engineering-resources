# echo-client.py

import socket
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('port')
args=parser.parse_args()

HOST = "127.0.0.1"  # The server's hostname or IP address
PORT = int(args.port)  # The port used by the server

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    while True:
        string = input()
        s.sendall(string.encode())
        data = s.recv(1024)
        print(f"Received {data!r}")
