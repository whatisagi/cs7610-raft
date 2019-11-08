import asyncio
import socket

class Connection:
    def __init__(self):
        self._socket = None

    def __enter__(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._socket.close()

    def print(self, a):
        print(a)

if __name__ == "__main__":
    with Connection() as con:
        con.print("hello world")
