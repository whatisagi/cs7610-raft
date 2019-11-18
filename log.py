#!/usr/bin/env python3

__all__ = ["GetOp", "PutOp", "NoOp"]

class LogItem:
    __slots__ = ["term", "command"]
    def __init__(self, term, command):
        self.term = term
        self.command = command

class GetOp(LogItem):
    __slots__ = ["key"]
    def __init__(self, term, key):
        super().__init__(term, "get")
        self.key = key
    def handle(self, server):
        return server.stateMachine[self.key]

class PutOp(LogItem):
    __slots__ = ["key", "value"]
    def __init__(self, term, key, value):
        super().__init__(term, "put")
        self.key = key
        self.value = value
    def handle(self, server):
        server.stateMachine[self.key] = self.value

class NoOp(LogItem):
    def __init__(self, term):
        super().__init__(term, "no-op")
    def handle(self, server):
        pass