#!/usr/bin/env python3

import asyncio
from config import Config
from connection import ServerConnection
from messages import *

class Server:
    __slots__ = ["_conn", "_id", "_server_num", "_loop"]

    def __init__(self, config=Config):
        self._conn = ServerConnection(config)
        self._id = self._conn.server_id
        self._server_num = len(config.SERVER_NAMES)
        self._loop = asyncio.get_event_loop()

    def test_handler(self, msg):
        print(self._id, ':', msg)

    def request_vote_handler(self, msg):
        pass

    def request_vote_reply_handler(self, msg):
        pass

    def append_entry_handler(self, msg):
        pass

    def append_entry_reply_handler(self, msg):
        pass

    def get_handler(self, msg):
        pass

    def put_handler(self, msg):
        pass

    async def server_handler(self):
        print("I'm Server", self._id)
        while True:
            msg = await self._conn.receive_message_from_server()
            msg.handle(self)

    async def client_handler(self):
        while True:
            msg = await self._conn.receive_message_from_client()
            msg.handle(self)

    def run(self):
        try:
            with self._conn:
                self._loop.create_task(Server.server_handler(self))
                self._loop.create_task(Server.client_handler(self))
                self._loop.run_forever()
        except (KeyboardInterrupt, SystemExit):
            print()
            print("Server", self._id, "crashes")
        finally:
            pending = [t for t in asyncio.Task.all_tasks()]
            for t in pending:
                t.cancel()
            self._loop.run_until_complete(asyncio.gather(*pending))
            self._loop.close()

if __name__ == "__main__":
    server = Server()
    server.run()