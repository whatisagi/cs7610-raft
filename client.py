#!/usr/bin/env python3

import json
import time
import threading
import queue
import asyncio
import sys
from config import Config
from connection import ClientConnection
from messages import *

DELAY = 3               # client timeout delay (in seconds)
NUM_SERVERS = int(sys.argv[1])  # total number of servers
CHOOSE_RANDOM_LEADER = True     # flag telling client to randomly choose new leader
SEND_NEW = True         # flag telling client to send a new instruction
SEND_SUCCESS = False    # flag telling client that last message was received by leader


class Client:
    def __init__(self):
        self.leader = 0
        self.msgId = 0
        self.sendQ = queue.Queue()
        self.recQ = queue.Queue()
        config = Config()
        self._conn = ClientConnection(config)
        self._loop = asyncio.get_event_loop()
        self._id = self._conn.client_id
    
    def find_leader(self):
        return self.leader

    def put(self, key, value):
        # push put message onto sendQ
        return None

    def get (self, key):
        # push get message onto sendQ
        return None

    async def tryInstruction(self):
        print("in sendInstruction") #debug
        #sendHandle = asyncio.Handle()
        global SEND_SUCCESS
        global SEND_NEW
        global DELAY
        while True:
            await asyncio.sleep(1)
            if not self.sendQ.empty() and SEND_NEW:
                msg_to_server = self.sendQ.get()
                SEND_NEW = False
            if not SEND_NEW:
                await asyncio.sleep(DELAY)
                if not SEND_SUCCESS:
                    await self.sendInstruction(msg_to_server)
            if SEND_SUCCESS:
                SEND_SUCCESS = False
                SEND_NEW = True

    async def sendInstruction(self, instruction):
        global NUM_SERVERS
        global CHOOSE_RANDOM_LEADER
        if CHOOSE_RANDOM_LEADER:
            print("choosing random leader") #debug
            self.leader = (self.leader + 1) % NUM_SERVERS
        CHOOSE_RANDOM_LEADER = True
        print("leader: ", self.leader) #debug
        self.msgId = self.msgId + 1
        instruction.messageId = self.msgId
        await instruction.handle(self) #debug
        await self._conn.send_message_to_server(instruction, self.leader)

    def test_handler(self, msg):
        print(self._id, ':', msg)

    def get_reply_handler(self, msg):
        pass

    async def get_handler(self, msg):
        print(self._id, ": get(", msg.key, ") , id: ", msg.messageId)

    async def put_handler(self, msg):
        print(self._id, ": put(", msg.key, ',', msg.value, ") , id: ", msg.messageId)

    def put_reply_handler(self, msg):
        pass

    async def generator(self, loop, stream):
        reader = asyncio.StreamReader(loop=loop)
        reader_protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda:reader_protocol, stream)
        while True:
            line = await reader.readline()
            if not line:
                break
            yield line.decode()

    async def inputInstruction(self):
        print("in input instruction") # debug
        async for line in Client.generator(self, self._loop, sys.stdin):
            input = line.split()
            if len(input) <= 0:
                print("bad instruction")
            elif input[0] == "put":
                if len(input) != 3:
                    pass
                else:
                    msg = Put(input[1], int(input[2]))
                    await msg.handle(self) #debug
                    self.sendQ.put(msg)
            elif input[0] == 'get':
                if len(input) != 2:
                    print("bad instruction")
                else: 
                    msg = Get(input[1])
                    await msg.handle(self) #debug
                    self.sendQ.put(msg)
            else: print("bad instruction")


    async def server_handler(self):
        print("I'm Client", self._id) #debug
        global SEND_SUCCESS
        global CHOOSE_RANDOM_LEADER
        while True:
            msg = await self._conn.receive_message_from_server()
            if not isinstance(msg, Message):
                print("received non-message")
            else:
                print("received message: id=", msg.messageId) #debug
                #await msg.handle(self) #debug
                if msg.notleader == False and msg.messageId == self.msgId:
                    print("recieved reply to last sent message") #debug
                    print(msg.messageId)
                    SEND_SUCCESS = True
                if msg.notleader == True and msg.messageId == self.msgId and SEND_SUCCESS == False:
                    print("responder not leader") #debug
                    if msg.leaderId is None:
                        print("msg.leaderId is None")
                        CHOOSE_RANDOM_LEADER = True
                    else:
                        print("msg.leaderId is not None")
                        CHOOSE_RANDOM_LEADER = False
                        self.leader = msg.leaderId

    def run(self):
        with self._conn:
            self._loop.create_task(Client.server_handler(self))
            self._loop.create_task(Client.tryInstruction(self))         
            self._loop.create_task(Client.inputInstruction(self))
            self._loop.run_forever()
        pending = [t for t in asyncio.Task.all_tasks()]
        for t in pending:
            t.cancel()
            with suppress(asyncio.CancelledError):
                self._loop.run_until_complete(t)
        self._loop.close()



# program is started with call to 
# python3 client.py <total number of servers>

if __name__ == "__main__":
    client = Client()
    client.run()