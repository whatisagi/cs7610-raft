#!/usr/bin/env python3

import asyncio
from config import Config
from connection import ServerConnection, ClientConnection

class Server:
    def __init__(self, config=Config):
        self._conn = ServerConnection(config)
        self._my_id = self._conn.server_id

    def run(self):
        with self._conn:
            self._conn._server_to_server_conn.print("hello world")

if __name__ == "__main__":
    server = Server()
    server.run()

