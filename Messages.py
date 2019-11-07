
class ServerMessage:
    messageId = 0

class AppendEntry(ServerMessage):
    term = 0
    leaderId = 0 
    prevLogIndex = 0
    prevLogTerm = 0
    entry = 0
    leaderCommit = 0

class RequestVote(ServerMessage):
    term = 0
    candidateId = 0
    lastLogIndex = 0
    lastLogTerm = 0

class AppendEntryReply(ServerMessage):
    term = 0
    success = True  

class RequestVoteReply(ServerMessage):
    term = 0
    voteGranted = True

#client-server messages

class Get:
    key = 0

class Put(Get):
    value = 0

class PutReply:
    notleader = True
    leaderId = 0
    success = True

class GetReply(PutReply):
    value = 0


    
