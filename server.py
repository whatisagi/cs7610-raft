#!/usr/bin/env python3

import asyncio
import pickle
from contextlib import suppress

from config import Config
from connection import ServerConnection
from messages import *

class Storage:
    __slots__ = ["_id"]

    def read(self, id):
        self._id = id
        try:
            with open("server"+str(id)+".storage", "rb") as f:
                currentTerm = pickle.load(f)
                votedFor = pickle.load(f)
                log = pickle.load(f)
        except OSError:
            currentTerm = 0
            votedFor = None
            log = []
        return (currentTerm, votedFor, log)

    def store(self, currentTerm, votedFor, log):
        with open("server"+str(self._id)+".storage", "wb") as f:
            pickle.dump(currentTerm, f)
            pickle.dump(votedFor, f)
            pickle.dump(log, f)

class Server:
    __slots__ = ["_conn", "_id", "_server_num", "_loop", "_storage", "currentTerm", "votedFor", "log", "commitIndex", "lastApplied", "nextIndex", "matchIndex"]

    def __init__(self, config=Config, storage=Storage()):
        self._conn = ServerConnection(config)
        self._id = self._conn.server_id
        self._server_num = len(config.SERVER_NAMES)
        self._loop = asyncio.get_event_loop()
        self._storage = storage
        self.currentTerm, self.votedFor, self.log = self._storage.read(self._id)
        self.commitIndex = 0
        self.lastApplied = 0

    async def test_handler(self, msg):
        print(self._id, ':', msg)

    async def request_vote_handler(self, msg):
        pass

    async def request_vote_reply_handler(self, msg):
        pass

    async def append_entry_handler(self, msg):
        pass

    async def append_entry_reply_handler(self, msg):
        pass

    async def get_handler(self, msg):
        pass

    async def put_handler(self, msg):
        pass

    async def server_handler(self):
        print("I'm Server", self._id)
        while True:
            msg = await self._conn.receive_message_from_server()
            msg.handle(self)

    async def client_handler(self):
        msg = Test(self._id)
        asyncio.gather(*(self._conn.send_message_to_server(msg, id) for id in range(self._server_num) if id != self._id))
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
            self._storage.store(self.currentTerm, self.votedFor, self.log)
            print()
            print("Server", self._id, "crashes")
        finally:
            pending = [t for t in asyncio.Task.all_tasks()]
            for t in pending:
                t.cancel()
                with suppress(asyncio.CancelledError):
                    self._loop.run_until_complete(t)
            self._loop.close()

if __name__ == "__main__":
    server = Server()
    server.run()