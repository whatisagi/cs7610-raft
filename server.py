#!/usr/bin/env python3

import asyncio
import pickle
import enum
import random
from contextlib import suppress
from typing import Optional, List, Dict, Tuple, Set

from config import Config
from connection import ServerConnection
from messages import Message, Test, AppendEntry, RequestVote, AppendEntryReply, RequestVoteReply, Get, Put, GetReply, PutReply
from log import LogItem, GetOp, PutOp, NoOp

__all__ = ["Server"]

class Storage:
    __slots__ = ["_id"]

    def read(self, id: int) -> Tuple[int, Optional[int], List[LogItem]]:
        self._id = id
        try:
            with open("server"+str(id)+".storage", "rb") as f:
                currentTerm: int = pickle.load(f)
                votedFor: Optional[int] = pickle.load(f)
                log: List[LogItem] = pickle.load(f)
        except OSError:
            currentTerm: int = 0
            votedFor: Optional[int] = 0
            log: List[LogItem] = [NoOp(0)]
        return (currentTerm, votedFor, log)

    def store(self, currentTerm: int, votedFor: Optional[int], log: List[LogItem]) -> None:
        with open("server"+str(self._id)+".storage", "wb") as f:
            pickle.dump(currentTerm, f)
            pickle.dump(votedFor, f)
            pickle.dump(log, f)

class State(enum.Enum):
    follower = "Follower"
    candidate = "Candidate"
    leader = "Leader"

class Server:
    __slots__ = ["_conn", "_id", "_server_num", "_loop", "_storage", "_voted_for_me", "_message_resend_timer", "_election_timer", "_heartbeat_timer", "_apply_notifier",
        "currentTerm", "votedFor", "log", "commitIndex", "lastApplied", "nextIndex", "matchIndex", "state", "stateMachine"]

    # methods for initialization
    def __init__(self, config: Config=Config, storage: Storage=Storage()) -> None:
        self._conn = ServerConnection(config)
        self._id: int = self._conn.server_id
        self._server_num = len(config.SERVER_NAMES)
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self._storage = storage
        self._voted_for_me: Set[int] = set()
        self._message_resend_timer: Dict[int, asyncio.Task] = {}
        self._election_timer: Optional[asyncio.Task] = None
        self._heartbeat_timer: List[asyncio.Task] = []
        self._apply_notifier: Optional[asyncio.Event] = None

    async def init(self) -> None:
        self.currentTerm, self.votedFor, self.log = self._storage.read(self._id)
        self.commitIndex = 0
        self.lastApplied = 0
        self.nextIndex: List[int] = [len(self.log) for i in range(self._server_num)]
        self.matchIndex: List[int] = [0 for i in range(self._server_num)]
        self.stateMachine: Dict[str, int] = {}
        if self.currentTerm == 0 and self._id == 0:
            self.log: List[LogItem] = [NoOp(0), GetOp(0, 'x'), PutOp(0, 'x', 4), GetOp(0, 'x'), PutOp(0, 'y', 3), PutOp(0, 'x', 5), GetOp(0, 'x')]
            await self.enter_leader_state()
        else:
            await self.enter_follower_state()

    # methods for sending and resending messages
    async def message_resender(self, msg: Message, id: int, timeout: float=Config.RESEND_TIMEOUT, try_limit: int=Config.TRY_LIMIT) -> None: #TO BE DETERMINED: Indefinitely retry or not?
        try:
            for _ in range(try_limit):
                await asyncio.sleep(timeout)
                await self._conn.send_message_to_server(msg, id)
        except asyncio.CancelledError:
            pass

    async def message_sender(self, msg: Message, id: int, resend: bool=True) -> None:
        await self._conn.send_message_to_server(msg, id)
        if resend:
            self._message_resend_timer[msg.messageId] = self._loop.create_task(self.message_resender(msg, id))

    # methods for sending heartbeats
    async def heartbeat_sender(self, id: int) -> None:
        try:
            while True:
                msg = AppendEntry(self.currentTerm, self._id, 0, 0, None, self.commitIndex)
                await self.message_sender(msg, id, False)
                await asyncio.sleep(Config.HEARTBEAT_TIMEOUT)
        except asyncio.CancelledError:
            pass

    # methods for handling requests and responses from servers
    async def test_handler(self, msg: Test) -> None:
        print(self._id, ':', msg)

    async def request_vote_handler(self, msg: RequestVote) -> None:
        voteGranted = False
        if msg.term >= self.currentTerm:
            if self.votedFor is None or self.votedFor == msg.candidateId:
                if msg.lastLogTerm > self.log[-1].term or ( msg.lastLogTerm == self.log[-1].term and msg.lastLogIndex >= len(self.log)-1):
                    voteGranted = True
                    self.currentTerm = msg.term
                    self.votedFor = msg.candidateId
                    self.reset_election_timer()
        
        if msg.term >= self.currentTerm:
            print("Received RequestVote from Server {} for term {} with ({},{}), {}grant".format(msg.candidateId, msg.term, msg.lastLogTerm, msg.lastLogIndex, "" if voteGranted else "not "))
        self._storage.store(self.currentTerm, self.votedFor, self.log) # persistent storage before voting
        reply_msg = RequestVoteReply(msg.messageId, self.currentTerm, voteGranted, self._id)
        await self.message_sender(reply_msg, msg.candidateId, False)

    async def request_vote_reply_handler(self, msg: RequestVoteReply) -> None:
        if self.state == State.candidate and msg.messageId in self._message_resend_timer:
            # cancel the resender
            self._message_resend_timer[msg.messageId].cancel()
            del self._message_resend_timer[msg.messageId]

            print("Received RequestVoteReply from Server {}, {}granted".format(msg.senderId, "" if msg.voteGranted else "not "))
            if msg.voteGranted:
                self._voted_for_me.add(msg.senderId)
                if len(self._voted_for_me)+1 > self._server_num // 2:
                    await self.enter_leader_state()

    async def append_entry_handler(self, msg: AppendEntry) -> None:
        success = False
        to_print_log = False
        if msg.term >= self.currentTerm:
            # become follower if in candidate state
            if self.state == State.candidate:
                self.votedFor = msg.leaderId
                await self.enter_follower_state()

            if msg.entry is None:
                # dealing with heartbeat
                self.reset_election_timer()
                print("Received heartbeat from Server {} for term {}".format(msg.leaderId, msg.term))
            elif msg.prevLogIndex <= len(self.log)-1 and self.log[msg.prevLogIndex].term == msg.prevLogTerm:
                success = True
                if msg.prevLogIndex < len(self.log)-1:
                    if self.log[msg.prevLogIndex+1].term != msg.entry.term:
                        self.log = self.log[:msg.prevLogIndex+1]
                if msg.prevLogIndex == len(self.log)-1:
                    self.log.append(msg.entry)
                    to_print_log = True
                self._storage.store(self.currentTerm, self.votedFor, self.log) # persistent storage before committing
                self.reset_election_timer()
            if msg.leaderCommit > self.commitIndex:
                oldCommitIndex = self.commitIndex
                self.commitIndex = min(msg.leaderCommit, len(self.log)-1)
                if self.commitIndex != oldCommitIndex:
                    to_print_log = True
                self.apply_entries()
        
        if msg.term >= self.currentTerm and msg.entry is not None:
            print("Received AppendEntry from Server {} for term {} with ({},{},{},{}), {}".format(msg.leaderId, msg.term, msg.prevLogIndex, msg.prevLogTerm, msg.entry, msg.leaderCommit, "success" if success else "fail"))
        if to_print_log:
            self.print_log()
        reply_msg = AppendEntryReply(msg.messageId, self.currentTerm, success, self._id)
        await self.message_sender(reply_msg, msg.leaderId, False)

    async def append_entry_reply_handler(self, msg: AppendEntryReply) -> None:
        if self.state == State.leader and msg.messageId in self._message_resend_timer:
            # cancel the resender
            self._message_resend_timer[msg.messageId].cancel()
            del self._message_resend_timer[msg.messageId]

            print("Received AppendEntryReply from Server {} for term {}, {}".format(msg.senderId, msg.term, "success" if msg.success else "fail"))
            if msg.success:
                self.matchIndex[msg.senderId] = self.nextIndex[msg.senderId]
                self.nextIndex[msg.senderId] += 1

                # update commitIndex and apply log entries
                N = self.matchIndex[msg.senderId]
                while N > self.commitIndex and self.log[N].term == self.currentTerm:
                    count = len([1 for id in range(self._server_num) if id != self._id and self.matchIndex[id] >= N])
                    if count+1 > self._server_num // 2:
                        self.commitIndex = N
                        self.apply_entries()
                        self.print_log()
                        break
                    N -= 1
            else:
                self.nextIndex[msg.senderId] -= 1
            
            # continue the process of appending entries
            if self.nextIndex[msg.senderId] <= len(self.log)-1:
                next_msg = AppendEntry(self.currentTerm, self._id, self.nextIndex[msg.senderId]-1, self.log[self.nextIndex[msg.senderId]-1].term, self.log[self.nextIndex[msg.senderId]], self.commitIndex)
                await self.message_sender(next_msg, msg.senderId)

    async def get_handler(self, msg: Get) -> None:
        reply_msg = None
        if self.state == State.leader:
            op = GetOp(self.currentTerm, msg.key)
            print("Received {} from client".format(op))
            commit_success = await self.op_handler(op)
            if commit_success: # successfully commit and apply the log entry
                res = op.handle(self)
                if res is not None: # key is in the state machine
                    reply_msg = GetReply(msg.messageId, False, self._id, True, res)
                else: # key is not in the state machine, fail
                    reply_msg = GetReply(msg.messageId, False, self._id, False, None)
        if reply_msg is None: # not leader when replying
            reply_msg = GetReply(msg.messageId, True, self.votedFor, False, None)
        await self._conn.send_message_to_client(reply_msg, 0)

    async def put_handler(self, msg: Put) -> None:
        reply_msg = None
        if self.state == State.leader:
            op = PutOp(self.currentTerm, msg.key, msg.value)
            print("Received {} from client".format(op))
            commit_success = await self.op_handler(op)
            if commit_success: # successfully commit and apply the log entry
                reply_msg = PutReply(msg.messageId, False, self._id, True)
        if reply_msg is None: # not leader when replying
            reply_msg = PutReply(msg.messageId, True, self.votedFor, False)
        await self._conn.send_message_to_client(reply_msg, 0)

    # methods for handling log entries
    def apply_entries(self) -> None:
        while self.lastApplied < self.commitIndex:
            self.lastApplied += 1
            self.log[self.lastApplied].handle(self)
        if self.commitIndex == len(self.log)-1 and self._apply_notifier is not None:
            # the latest log entry is committed, can respond to client now
            self._apply_notifier.set()

    async def op_handler(self, op: LogItem) -> bool:
        self.log.append(op)
        self._storage.store(self.currentTerm, self.votedFor, self.log) # persistent storage before committing
        index = len(self.log)-1
        self._apply_notifier = asyncio.Event()

        # sending initial AppendEntry RPCs
        for id in range(self._server_num):
            if id != self._id and self.nextIndex[id] <= index:
                msg = AppendEntry(self.currentTerm, self._id, self.nextIndex[id]-1, self.log[self.nextIndex[id]-1].term, self.log[self.nextIndex[id]], self.commitIndex)
                await self.message_sender(msg, id)

        # waiting for the entry to be committed
        # but it may return before really committed when a leader turns into a follwer
        await self._apply_notifier
        return self.commitIndex >= index

    def print_log(self) -> None:
        print("log:", end=" ")
        for i in range(self.commitIndex):
            print(self.log[i], end=',')
        print(self.log[self.commitIndex], end='|')
        for i in range(self.commitIndex+1, len(self.log)-1):
            print(self.log[i], end=",")
        if self.commitIndex < len(self.log)-1:
            print(self.log[-1])
        else:
            print()
        print(self.stateMachine)

    # methods for changing state
    async def exit_current_state(self) -> None:
        # cancel all the resend timers when exit
        for t in self._heartbeat_timer:
            t.cancel()
        for t in self._message_resend_timer.values():
            t.cancel()
        self._apply_notifier.set()
        await asyncio.sleep(0)
        self._heartbeat_timer = []
        self._message_resend_timer = {}
        self._apply_notifier = {}

    async def enter_follower_state(self) -> None:
        await self.exit_current_state()
        self.state = State.follower
        self.reset_election_timer()
        print("Server {}: follower of term {}".format(self._id, self.currentTerm))

    async def enter_candidate_state(self) -> None:
        await self.exit_current_state()
        self.state = State.candidate
        self._voted_for_me = set()
        self.currentTerm += 1
        self.votedFor = self._id
        self.reset_election_timer(True)
        print("Server {}: starting election for term {}".format(self._id, self.currentTerm))

        for id in range(self._server_num):
            if id != self._id:
                msg = RequestVote(self.currentTerm, self._id, len(self.log)-1, self.log[-1].term)
                await self.message_sender(msg, id)

    async def enter_leader_state(self) -> None:
        await self.exit_current_state()
        self.state = State.leader
        self.cancel_election_timer()
        self.nextIndex = [len(self.log) for i in range(self._server_num)]
        self.matchIndex = [0 for i in range(self._server_num)]
        print("Server {}: leader of term {}".format(self._id, self.currentTerm))

        # send out hearbeats immediately and indefinitely
        for id in range(self._server_num):
            if id != self._id:
                self._heartbeat_timer.append(self._loop.create_task(self.heartbeat_sender(id)))
        
        # not really needed; for testing mainly
        op = NoOp(self.currentTerm)
        await self.op_handler(op)

    # methods for managing election timer
    async def election_timeout(self) -> None:
        try:
            timeout = random.uniform(Config.ELECTION_TIMEOUT, 2 * Config.ELECTION_TIMEOUT)
            await asyncio.sleep(timeout)
            await self.enter_candidate_state()
        except asyncio.CancelledError:
            pass

    def cancel_election_timer(self, timeouted: bool=False) -> None:
        if self._election_timer is not None and not timeouted:
            self._election_timer.cancel()

    def reset_election_timer(self, timeouted: bool=False) -> None:
        self.cancel_election_timer(timeouted)
        self._election_timer = self._loop.create_task(self.election_timeout())

    # main methods for interacting with servers and client
    async def server_handler(self):
        while True:
            msg = await self._conn.receive_message_from_server()
            # if msg.term == 1:
            #     continue
            # update term immediately
            if msg.term > self.currentTerm:
                self.currentTerm = msg.term
                self.votedFor = None
                await self.enter_follower_state()
            await msg.handle(self)

    async def client_handler(self):
        while True:
            msg = await self._conn.receive_message_from_client()
            await msg.handle(self)

    def run(self):
        try:
            with self._conn:
                self._loop.create_task(self.init())
                self._loop.create_task(self.server_handler())
                self._loop.create_task(self.client_handler())
                self._loop.run_forever()
        except (KeyboardInterrupt, SystemExit):
            #self._storage.store(self.currentTerm, self.votedFor, self.log)
            print()
            print("Server", self._id, "crashes")
        finally:
            # gracefully shutdown all the tasks
            pending = [t for t in asyncio.Task.all_tasks()]
            for t in pending:
                t.cancel()
                with suppress(asyncio.CancelledError):
                    self._loop.run_until_complete(t)
            self._loop.close()

if __name__ == "__main__":
    server = Server()
    server.run()