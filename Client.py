import json
import threading

class Client:
    leader = 0

    def f(self, int):
        return int + 75
    
    def find_leader(self):
        return self.leader

    def put(self, key, value):
        """
        send put message to leader
        if...
            response times out
                increment leader index and loop
            response is false
                try again
            response is success
                return 
        """
        return None

    def get (self, key):
        # similar to put
        return None

