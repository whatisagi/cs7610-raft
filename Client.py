import json
import time
import Messages
import threading
import Queue

class Client:
    leader = 0
    msgId = 0
    sendQ = Queue.Queue()
    recQ = Queue.Queue()
    
    def find_leader(self):
        return self.leader

    def put(self, key, value):
        # push put message onto sendQ
        return None

    def get (self, key):
        # push get message onto sendQ
        return None

    def sendInstruction(self):
        # should be threaded to run continuously
        """  
        while true
            if sendQ is not empty
                pop sendQ
                sendSuccess = false
                while (not sendSuccess)
                    set ID of message to send (using msgID)
                    send instruction to self.leader
                    time.sleep(3): sleep for 3 seconds
                    pop all elements of recQ with lower msgID: these are ignored
                    if...
                        recQ is empty, i.e. response timed out
                            increment leader index 
                            increment msgId 
                        response is false
                            update leader index to sent index
                            increment msgId
                        response is success
                            sendSuccess = true
        """
        return None

    def receiveInstruction(self):
        # should be threaded
        # push message onto recQ
        return None

