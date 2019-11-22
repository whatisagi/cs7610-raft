#!/usr/bin/env python3

import json
import time
import threading
import queue
import asyncio
import sys
import random
from contextlib import suppress
from config import Config
from connection import ClientConnection
from messages import *

DELAY = 3               # client timeout delay (in seconds)
CHOOSE_RANDOM_LEADER = True     # flag telling client to randomly choose new leader

SEND_NEW = True         # flag telling client to send a new instruction
SEND_SUCCESS = False    # flag telling client that last message was received by leader

ADD_NEW = True  # flag telling client to add new servers
ADD_SUCCESS = False # flag telling client that last requested servers were added

ALL_SENT_MESSAGES = {} # dictionary to store all messages sent keyed by message ID


class Client:
    def __init__(self):
        self.leader = 0
        self.msgId = 0
        self.addmsgId = 0
        self.sendQ = queue.Queue()
        self.recQ = queue.Queue()
        self.addQ = queue.Queue()
        self.config = Config()
        self._conn = ClientConnection(self.config)
        self._loop = asyncio.get_event_loop()
        self._id = self._conn.client_id
    
    def find_leader(self):
        return self.leader

    async def tryInstruction(self):
        global SEND_SUCCESS
        global SEND_NEW
        global DELAY
        global CHOOSE_RANDOM_LEADER
        while True:
            await asyncio.sleep(1)
            if not self.sendQ.empty() and SEND_NEW:
                msg_to_server = self.sendQ.get()
                CHOOSE_RANDOM_LEADER = False
                await self.sendInstruction(msg_to_server)
                SEND_NEW = False
            if not SEND_NEW:
                await asyncio.sleep(DELAY)
                if not SEND_SUCCESS:
                    await self.sendInstruction(msg_to_server)
            if SEND_SUCCESS:
                SEND_SUCCESS = False
                SEND_NEW = True

    async def sendInstruction(self, instruction):
        global CHOOSE_RANDOM_LEADER
        global ALL_SENT_MESSAGES
        if CHOOSE_RANDOM_LEADER:
            #print("choosing random leader (get/put)") #debug
            self.leader = random.choice(list(self.config.INIT_SERVER_CONFIG))
        CHOOSE_RANDOM_LEADER = True
        #print("sending get/put to leader: ", self.leader) #debug
        self.msgId = self.msgId + 1
        instruction.messageId = self.msgId
        ALL_SENT_MESSAGES[self.msgId] = instruction
        #print("message", ALL_SENT_MESSAGES[self.msgId].messageId, "stored in dictionary at index", self.msgId) #debug
        #await instruction.handle(self) #debug
        await self._conn.send_message_to_server(instruction, self.leader)

    async def tryAdd(self):
        global ADD_SUCCESS
        global ADD_NEW
        global DELAY
        while True:
            await asyncio.sleep(1)
            if not self.addQ.empty() and ADD_NEW:
                msg_to_server = self.addQ.get()
                CHCHOOSE_RANDOM_LEADER = False
                await self.sendAddServers(msg_to_server)
                ADD_NEW = False
            if not ADD_NEW:
                await asyncio.sleep(DELAY)
                if not ADD_SUCCESS:
                    await self.sendAddServers(msg_to_server)
            if ADD_SUCCESS:
                ADD_SUCCESS = False
                ADD_NEW = True

    async def sendAddServers(self, instruction):
        global CHOOSE_RANDOM_LEADER
        global ALL_SENT_MESSAGES
        if CHOOSE_RANDOM_LEADER:
            #print("choosing random leader (add)") #debug
            self.leader = random.choice(list(self.config.INIT_SERVER_CONFIG))
        CHOOSE_RANDOM_LEADER = True
        print("sending add message to leader: ", self.leader) #debug
        self.msgId = self.msgId + 1
        self.addmsgId = self.msgId
        instruction.messageId = self.addmsgId
        ALL_SENT_MESSAGES[self.msgId] = instruction
        #print("message", ALL_SENT_MESSAGES[self.msgId].messageId, "stored in dictionary at index", self.msgId) #debug
        #await instruction.handle(self) #debug
        await self._conn.send_message_to_server(instruction, self.leader)
          
    def get_reply_handler(self, msg):
         print("getreply id:", msg.messageId)

    def put_reply_handler(self, msg):
         print("putreply id:", msg.messageId)

    def add_reply_handler(self,msg):
        print("addreply id:", msg.messageId)

    async def get_handler(self, msg):
        print("get(", msg.key, ") , id:", msg.messageId)

    async def put_handler(self, msg):
        print("put(", msg.key, ',', msg.value, ") , id:", msg.messageId)

    async def add_handler(self, msg):
        print("add[", end='')
        for i in range ( len(msg.servers) ): 
            if i == 0:
                print(msg.servers[i], end='')
            else:
                print(",", msg.servers[i], end='')
        print("], id:", msg.messageId)

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
        #print("in input instruction") # debug
        async for line in Client.generator(self, self._loop, sys.stdin):
            input = line.split()
            if len(input) <= 0:
                pass
            elif input[0] == "put":
                if len(input) != 3:
                    print("bad instruction")
                else:
                    msg = Put(input[1], int(input[2]))
                    #await msg.handle(self) #debug
                    self.sendQ.put(msg)
            elif input[0] == 'get':
                if len(input) != 2:
                    print("bad instruction")
                else: 
                    msg = Get(input[1])
                    #await msg.handle(self) #debug
                    self.sendQ.put(msg)
            elif input[0] == 'add':
                new_servers = []
                for i in range (1, len(input)):
                    new_servers = new_servers + [int(input[i])]                    
                msg = AddServers(set(new_servers))
                #await msg.handle(self) #debug
                self.addQ.put(msg)
            else: print("bad instruction")

    async def server_handler(self):
        print("I'm Client", self._id)
        global SEND_SUCCESS
        global CHOOSE_RANDOM_LEADER
        global ALL_SENT_MESSAGES
        while True:
            msg = await self._conn.receive_message_from_server()
            #await msg.handle(self) #debug
            sent_message = ALL_SENT_MESSAGES[msg.messageId]
            if msg.messageId == self.msgId and not msg.notleader:
                if isinstance(msg, GetReply):
                    #print("received reply to message ", msg.messageId) #debug
                    if  msg.success:
                        if msg.value is None:
                            print(sent_message.key, "-> <no value in store>") 
                        else:
                            print(sent_message.key, "->", msg.value)
                        SEND_SUCCESS = True 
                if isinstance(msg, PutReply):
                    #print("received reply to message ", msg.messageId) #debug
                    #if not msg.success:
                        #print("(",sent_message.key, ',', sent_message.value, ") not stored, retrying") #debug
                    if msg.success:
                        #print("successfully stored (",sent_message.key, ',', sent_message.value, ")") #debug
                        SEND_SUCCESS = True # we retry until key-value pair is stored
            if isinstance(msg, AddServersReply) and msg.messageId == self.addmsgId and not msg.notleader:
                print("received reply to message ", msg.messageId) #debug
                #if not msg.success:
                    #print("servers not added, retrying")
                if msg.success:
                    print("servers successfully added")
                    ADD_SUCCESS = True # we retry until servers are added
            if (msg.messageId == self.msgId or msg.messageId == self.addmsgId) and msg.notleader and (not SEND_SUCCESS or not ADD_SUCCESS):
                #print("responder not leader") #debug
                if msg.leaderId is None:
                    #print("msg.leaderId is not known") #debug
                    CHOOSE_RANDOM_LEADER = True
                else:
                    #print("msg.leaderId is known") #debug
                    CHOOSE_RANDOM_LEADER = False
                    self.leader = msg.leaderId

    def run(self):
        try:
            with self._conn:
                self._loop.create_task(Client.server_handler(self))
                self._loop.create_task(Client.tryInstruction(self))         
                self._loop.create_task(Client.inputInstruction(self))
                self._loop.create_task(Client.tryAdd(self))
                self._loop.run_forever()
        except (KeyboardInterrupt, SystemExit):
            print()
            #print("Client", self._id, "crashes")
        finally:
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