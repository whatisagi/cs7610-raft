#!/usr/bin/env python3

import asyncio
from config import Config
from connection import ServerConnection, ClientConnection

class Server:
    __slots__ = ["_conn", "_my_id", "_server_num"]

    def __init__(self, config=Config):
        self._conn = ServerConnection(config)
        self._my_id = self._conn.server_id
        self._server_num = len(config.SERVER_NAMES)

    def run(self):
        with self._conn:
            print(self._my_id, self._server_num)

if __name__ == "__main__":
    server = Server()
    server.run()