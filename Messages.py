class Message:
    messageId = 0

# server - server messages

class AppendEntry(Message):
    term = 0
    leaderId = 0 
    prevLogIndex = 0
    prevLogTerm = 0
    entry = 0
    leaderCommit = 0

class RequestVote(Message):
    term = 0
    candidateId = 0
    lastLogIndex = 0
    lastLogTerm = 0

class AppendEntryReply(Message):
    term = 0
    success = True  

class RequestVoteReply(Message):
    term = 0
    voteGranted = True

#client-server messages

class Get(Message):
    key = 0

class Put(Message):
    key = 0
    value = 0

class PutReply(Message):
    notleader = True
    leaderId = 0
    success = True

class GetReply(Message):
    notleader = True
    leaderId = 0
    success = True
    value = 0

