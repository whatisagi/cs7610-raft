__all__ = ["Config"]

class Config:
    SERVER_PORT = 11110
    CLIENT_PORT = 11120
    BUF_SIZE = 4096
    MAX_SERVER = 10
    MAX_CLIENT = 1
    TRY_LIMIT = 100
    RESEND_TIMEOUT = 0.5
    ELECTION_TIMEOUT = 10.0
    HEARTBEAT_TIMEOUT = 5.0

    SERVER_NAMES = ["vdi-linux-031.ccs.neu.edu", "vdi-linux-032.ccs.neu.edu", "vdi-linux-033.ccs.neu.edu"]
    CLIENT_NAMES = ["vdi-linux-030.ccs.neu.edu"]