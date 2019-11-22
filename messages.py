#!/usr/bin/env python3

from typing import Generator, Optional, Set, TYPE_CHECKING
if TYPE_CHECKING:
    from log import LogItem
    from server import Server

__all__ = ["Message", "Test", "AppendEntry", "RequestVote", "AppendEntryReply", "RequestVoteReply", "Get", "Put", "AddServers", "GetReply", "PutReply", "AddServersReply"]

def messageId_generator_fun() -> Generator[int, None, None]:
    id = 0
    while True:
        yield id
        id += 1

messageId_generator = messageId_generator_fun()

class Message:
    __slots__ = ["messageId"]
    def __init__(self):
        self.messageId: int = next(messageId_generator)

class Test(Message):
    __slots__ = ["senderId"]
    def __init__(self, senderId: int) -> None:
        super().__init__()
        self.senderId = senderId
    def __str__(self) -> str:
        return str(self.messageId) + ' ' + str(self.senderId)
    async def handle(self, server: "Server") -> None:
        await server.test_handler(self)

# server - server messages

class AppendEntry(Message):
    __slots__ = ["term", "leaderId", "prevLogIndex", "prevLogTerm", "entry", "leaderCommit"]
    def __init__(self, term: int, leaderId: Optional[int], prevLogIndex: int, prevLogTerm: int, entry: "Optional[LogItem]", leaderCommit: int) -> None:
        super().__init__()
        self.term = term
        self.leaderId = leaderId
        self.prevLogIndex = prevLogIndex
        self.prevLogTerm = prevLogTerm
        self.entry = entry
        self.leaderCommit = leaderCommit
    async def handle(self, server: "Server") -> None:
        await server.append_entry_handler(self)

class RequestVote(Message):
    __slots__ = ["term", "candidateId", "lastLogIndex", "lastLogTerm"]
    def __init__(self, term: int, candidateId: int, lastLogIndex: int, lastLogTerm: int) -> None:
        super().__init__()
        self.term = term
        self.candidateId = candidateId
        self.lastLogIndex = lastLogIndex
        self.lastLogTerm = lastLogTerm
    async def handle(self, server: "Server") -> None:
        await server.request_vote_handler(self)

class AppendEntryReply(Message):
    __slots__ = ["term", "success", "senderId"]
    def __init__(self, messageId: int, term: int, success: bool, senderId: int) -> None:
        self.messageId = messageId
        self.term = term
        self.success = success
        self.senderId = senderId
    async def handle(self, server: "Server") -> None:
        await server.append_entry_reply_handler(self)

class RequestVoteReply(Message):
    __slots__ = ["term", "voteGranted", "senderId"]
    def __init__(self, messageId: int, term: int, voteGranted: bool, senderId: int) -> None:
        self.messageId = messageId
        self.term = term
        self.voteGranted = voteGranted
        self.senderId = senderId
    async def handle(self, server: "Server") -> None:
        await server.request_vote_reply_handler(self)

#client-server messages

class Get(Message):
    __slots__ = ["key"]
    def __init__(self, key: str) -> None:
        super().__init__()
        self.key = key
    async def handle(self, server: "Server") -> None:
        await server.get_handler(self)

class Put(Message):
    __slots__ = ["key", "value"]
    def __init__(self, key: str, value: int) -> None:
        super().__init__()
        self.key = key
        self.value = value
    async def handle(self, server: "Server") -> None:
        await server.put_handler(self)

class AddServers(Message):
    __slots__ = ["servers"]
    def __init__(self, servers: Set[int]) -> None:
        super().__init__()
        self.servers = servers
    async def handle(self, server: "Server") -> None:
        await server.add_servers_handler(self)

class GetReply(Message):
    __slots__ = ["notleader", "leaderId", "success", "value"]
    def __init__(self, messageId: int, notleader: bool, leaderId: Optional[int], success: bool, value: Optional[int]) -> None:
        self.messageId = messageId
        self.notleader = notleader
        self.leaderId = leaderId
        self.success = success
        self.value = value
    async def handle(self, client) -> None:
        client.get_reply_handler(self)

class PutReply(Message):
    __slots__ = ["notleader", "leaderId", "success"]
    def __init__(self, messageId: int, notleader: bool, leaderId: Optional[int], success: bool) -> None:
        self.messageId = messageId
        self.notleader = notleader
        self.leaderId = leaderId
        self.success = success
    async def handle(self, client) -> None:
        client.put_reply_handler(self)

class AddServersReply(Message):
    __slots__ = ["notleader", "leaderId", "success", "servers"]
    def __init__(self, messageId: int, notleader: bool, leaderId: Optional[int], success: bool, servers: Set[int]) -> None:
        self.messageId = messageId
        self.notleader = notleader
        self.leaderId = leaderId
        self.success = success
        self.servers = servers
    async def handle(self, client) -> None:
        pass

if __name__ == "__main__":
    import pickle

    data = pickle.dumps(Test(0))
    msg2 = Test(1)
    data2 = pickle.dumps(msg2)
    msg3 = Test(2)
    data3 = pickle.dumps(msg3)

    msg1 = pickle.loads(data)
    msg2 = pickle.loads(data2)
    msg3 = pickle.loads(data3)

    print(msg1.messageId, msg1.senderId)
    print(msg2.messageId, msg2.senderId)
    print(msg3.messageId, msg3.senderId)