#!/usr/bin/env python3

import asyncio
import pickle
import enum
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
            votedFor = 0
            log = [{term: 0}]
        return (currentTerm, votedFor, log)

    def store(self, currentTerm, votedFor, log):
        with open("server"+str(self._id)+".storage", "wb") as f:
            pickle.dump(currentTerm, f)
            pickle.dump(votedFor, f)
            pickle.dump(log, f)

class State(enum.Enum):
    follower = "Follower"
    candidate = "Candidate"
    leader = "Leader"

class Server:
    __slots__ = ["_conn", "_id", "_server_num", "_loop", "_storage", "currentTerm", "votedFor", "log", "commitIndex", "lastApplied", "nextIndex", "matchIndex", "state"]

    def __init__(self, config=Config, storage=Storage()):
        self._conn = ServerConnection(config)
        self._id = self._conn.server_id
        self._server_num = len(config.SERVER_NAMES)
        self._loop = asyncio.get_event_loop()
        self._storage = storage
        self.currentTerm, self.votedFor, self.log = self._storage.read(self._id)
        self.commitIndex = 0
        self.lastApplied = 0
        if self.currentTerm == 0 and self._id == 0:
            self.state = State.leader
        else:
            self.enter_follower_state()

    async def test_handler(self, msg):
        print(self._id, ':', msg)

    async def request_vote_handler(self, msg):
        print("Received RequestVote from Server", msg.candidateId, "for term", msg.term, "with", msg.lastLogTerm, msg.lastLogIndex)
        reply_msg = RequestVoteReply(msg.messageId, self.currentTerm, False)
        if self.state == State.follower and msg.term >= self.currentTerm:
            if self.votedFor is None or self.votedFor == msg.candidateId:
                if msg.lastLogTerm > self.log[-1]['term'] or ( msg.lastLogTerm == self.log[-1]['term'] and msg.lastLogIndex >= len(self.log)):
                    reply_msg = RequestVoteReply(msg.messageId, self.currentTerm, True)
                    self.currentTerm = msg.term
                    self.votedFor = msg.candidateId
        await self._conn.send_message_to_server(reply_msg, msg.candidateId)

    async def request_vote_reply_handler(self, msg):
        #cancel the resender
        
        if msg.voteGranted:
            pass

    async def append_entry_handler(self, msg):
        pass

    async def append_entry_reply_handler(self, msg):
        pass

    async def get_handler(self, msg):
        pass

    async def put_handler(self, msg):
        pass

    def enter_follower_state(self):
        self.state = State.follower

    def enter_candidate_state(self):
        pass

    async def server_handler(self):
        print("I'm Server", self._id)
        while True:
            msg = await self._conn.receive_message_from_server()
            self._storage.store(self.currentTerm, self.votedFor, self.log)
            await msg.handle(self)
            if msg.term > self.currentTerm:
                self.currentTerm = msg.term
                self.votedFor = None
                self.enter_follower_state()

    async def client_handler(self):
        while True:
            msg = await self._conn.receive_message_from_client()
            await msg.handle(self)

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