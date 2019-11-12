#!/usr/bin/env python3

__all__ = ["Test", "AppendEntry", "RequestVote", "AppendEntryReply", "RequestVoteReply", "Get", "Put", "GetReply", "PutReply"]

def messageId_generator_fun():
    id = 0
    while True:
        yield id
        id += 1

messageId_generator = messageId_generator_fun()

class Message:
    __slots__ = ["messageId"]
    def __init__(self):
        self.messageId = next(messageId_generator)

class Test(Message):
    __slots__ = ["senderId"]
    def __init__(self, senderId):
        super().__init__()
        self.senderId = senderId
    def __str__(self):
        return str(self.messageId) + ' ' + str(self.senderId)
    def handle(self, server):
        server.test_handler(self)

# server - server messages

class AppendEntry(Message):
    __slots__ = ["term", "leaderId", "prevLogIndex", "prevLogTerm", "entry", "leaderCommit"]
    def __init__(self, term, leaderId, prevLogIndex, prevLogTerm, entry, leaderCommit):
        super().__init__()
        self.term = term
        self.leaderId = leaderId
        self.prevLogIndex = prevLogIndex
        self.prevLogTerm = prevLogTerm
        self.entry = entry
        self.leaderCommit = leaderCommit

class RequestVote(Message):
    __slots__ = ["term", "candidateId", "lastLogIndex", "lastLogTerm"]
    def __init__(self, term, candidateId, lastLogIndex, lastLogTerm):
        super().__init__()
        self.term = term
        self.candidateId = candidateId
        self.lastLogIndex = lastLogIndex
        self.lastLogTerm = lastLogTerm

class AppendEntryReply(Message):
    __slots__ = ["term", "success"]
    def __init__(self, term, success):
        super().__init__()
        self.term = term
        self.success = success

class RequestVoteReply(Message):
    __slots__ = ["term", "voteGranted"]
    def __init__(self, term, voteGranted):
        super().__init__()
        self.term = term
        self.voteGranted = voteGranted

#client-server messages

class Get(Message):
    __slots__ = ["key"]
    def __init__(self, key):
        super().__init__()
        self.key = key

class Put(Message):
    __slots__ = ["key", "value"]
    def __init__(self, key, value):
        super().__init__()
        self.key = key
        self.value = value

class GetReply(Message):
    __slots__ = ["notleader", "leaderId", "success", "value"]
    def __init__(self, notleader, leaderId, success, value):
        super().__init__()
        self.notleader = notleader
        self.leaderId = leaderId
        self.success = success
        self.value = value

class PutReply(Message):
    __slots__ = ["notleader", "leaderId", "success"]
    def __init__(self, notleader, leaderId, success):
        super().__init__()
        self.notleader = notleader
        self.leaderId = leaderId
        self.success = success

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