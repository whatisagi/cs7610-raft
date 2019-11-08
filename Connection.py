#!/usr/bin/env python3

import asyncio
import socket

class network_config:
    SERVER_PORT = 1111
    CLIENT_PORT = 1112

class Connection:
    __slots__ = ["_socket", "_server_host_names"]

    def __init__(self, server_host_names):
        self._socket = None
        self._server_host_names = server_host_names

    def __enter__(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._socket.close()

    def print(self, a):
        print(a)

    async def send_message(self):
        pass

class ServerConnection(Connection):
    __slots__ = ["_client_host_names"]

    def __init__(self, server_host_names, client_host_names):
        super().__init__(server_host_names)
        self._client_host_names = client_host_names

    async def send_to_client(self):
        pass

class ClientConnection(Connection):
    pass

if __name__ == "__main__":
    with ServerConnection(["127.0.0.1"], ["127.0.0.2"]) as con:
        con.print("hello world")
