#!/usr/bin/env python3

import asyncio
import socket

from config import Config

__all__ = ["ServerConnection", "ClientConnection"]

class Resolver:
    """
    For resolving host names to IP addresses, and getting my id
    """
    __slots__ = ["_my_id", "_my_address", "_host_addresses"]

    def __init__(self, host_names):
        self._my_id = None
        my_name = socket.gethostname()
        self._my_address = socket.gethostbyname(my_name)
        self._host_addresses = list(map(socket.gethostbyname, host_names))
    
    @property
    def my_id(self):
        if not self._my_id:
            self._my_id = self._host_addresses.index(self._my_address)
        return self._my_id

    @property
    def host_addresses(self):
        return self._host_addresses

class Connection:
    """
    ContextManager class for asynchronous connectless UDP "connection"
    """
    __slots__ = ["_socket", "_resolver", "_host_addresses", "_port"]

    def __init__(self, host_names, port):
        self._socket = None
        self._resolver = Resolver(host_names)
        self._host_addresses = self._resolver.host_addresses
        self._port = port

    def __enter__(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.bind(("localhost", self._port))
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._socket.close()

class ClientConnection:
    """
    ContextManager class for connections used by Client
    """
    __slots__ = ["_client_to_server_conn", "_client_resolver"]

    def __init__(self, config=Config):
        self._client_to_server_conn = Connection(config.SERVER_NAMES, config.CLIENT_PORT)
        self._client_resolver = Resolver(config.CLIENT_NAMES)
    
    @property
    def client_id(self):
        return self._client_resolver.my_id

    def __enter__(self):
        self._client_to_server_conn.__enter__()
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self._client_to_server_conn.__exit__(exc_type, exc_value, traceback)

class ServerConnection:
    """
    ContextManager class for connections used by Server
    """
    __slots__ = ["_server_to_server_conn", "_server_to_client_conn", "_server_resolver"]

    def __init__(self, config=Config):
        self._server_to_server_conn = Connection(config.SERVER_NAMES, config.SERVER_PORT)
        self._server_to_client_conn = Connection(config.CLIENT_NAMES, config.CLIENT_PORT)
        self._server_resolver = Resolver(config.SERVER_NAMES)

    @property
    def server_id(self):
        return self._server_resolver.my_id

    def __enter__(self):
        self._server_to_server_conn.__enter__()
        self._server_to_client_conn.__enter__()
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self._server_to_server_conn.__exit__(exc_type, exc_value, traceback)
        self._server_to_client_conn.__exit__(exc_type, exc_value, traceback)


if __name__ == "__main__":
    # test
    with Connection(Config.SERVER_NAMES, Config.SERVER_PORT) as conn:
        conn.print("hello world")
    with ServerConnection() as conn:
        conn._server_to_server_conn.print("hello world")
    with ClientConnection() as conn:
        conn._client_to_server_conn.print("hello world")