#!/usr/bin/env python3

import asyncio
from config import Config
from connection import ServerConnection, ClientConnection

class Server:
    __slots__ = ["_conn", "_my_id", "_server_num", "_loop"]

    def __init__(self, config=Config):
        self._conn = ServerConnection(config)
        self._my_id = self._conn.server_id
        self._server_num = len(config.SERVER_NAMES)
        self._loop = asyncio.get_event_loop()

    async def server_handler(self):
        print(self._my_id)
        msg2 = str(self._my_id)
        while True:
            await asyncio.gather([self._conn.send_data_to_server(msg2.encode(), id)
                for id in iter(range(self._server_num)) if id != self._my_id])
            await asyncio.sleep(1)
            data = await self._conn.receive_data_from_server()
            msg = data.decode()
            print(msg)
            msg2 = msg + str(self._my_id)

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