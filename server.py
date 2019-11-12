#!/usr/bin/env python3

import asyncio
from config import Config
from connection import ServerConnection
from messages import *

class Server:
    __slots__ = ["_conn", "_my_id", "_server_num", "_loop"]

    def __init__(self, config=Config):
        self._conn = ServerConnection(config)
        self._my_id = self._conn.server_id
        self._server_num = len(config.SERVER_NAMES)
        self._loop = asyncio.get_event_loop()

    async def server_handler(self):
        print("I'm Server", self._my_id)
        msg = Test(self._my_id)
        while True:
            await asyncio.gather(*(self._conn.send_message_to_server(msg, id)
                for id in range(self._server_num) if id != self._my_id))
            await asyncio.sleep(1)
            msg = await self._conn.receive_message_from_server()
            print(msg)
            msg = Test(self._my_id)

    async def client_handler(self):
        while True:
            await asyncio.sleep(5)
            print('....................')

    def run(self):
        with self._conn:
            self._loop.run_until_complete(asyncio.gather(Server.server_handler(self), Server.client_handler(self)))

if __name__ == "__main__":
    server = Server()
    server.run()