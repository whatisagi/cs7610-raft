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

    async def server_handler(self):
        if self._my_id == 0:
            data = u"1".encode()
            await self._conn.send_data_to_server(data, 1)
        while True:
            data = await self._conn.receive_data_from_server()
            msg = data.decode()
            print(msg)
            msg2 = msg + u"1"
            async for id in iter(range(self._server_num)):
                if id != self._my_id:
                    await self._conn.send_data_to_server(msg2.encode(), id)

    async def client_handler(self):
        while True:
            await asyncio.sleep(5)
            print('...')

    def run(self):
        with self._conn:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(asyncio.gather(Server.server_handler(self), Server.client_handler(self)))

if __name__ == "__main__":
    server = Server()
    server.run()