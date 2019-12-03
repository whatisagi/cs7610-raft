__all__ = ["Config"]

class Config:
    SERVER_PORT = 11110
    CLIENT_PORT = 11120
    BUF_SIZE = 4096
    MAX_SERVER = 10
    MAX_CLIENT = 1
    TRY_LIMIT = 100
    RESEND_TIMEOUT = 0.2
    ELECTION_TIMEOUT = 1.0
    HEARTBEAT_TIMEOUT = 0.2

    SERVER_NAMES = ["vdi-linux-030.ccs.neu.edu", "vdi-linux-031.ccs.neu.edu", "vdi-linux-032.ccs.neu.edu", "vdi-linux-033.ccs.neu.edu",
                    "vdi-linux-034.ccs.neu.edu", "vdi-linux-035.ccs.neu.edu", "vdi-linux-036.ccs.neu.edu", "vdi-linux-037.ccs.neu.edu",
                    "vdi-linux-038.ccs.neu.edu", "vdi-linux-039.ccs.neu.edu"]
    CLIENT_NAMES = ["vdi-linux-040.ccs.neu.edu"]
    INIT_SERVER_CONFIG = {0,1,2}

    # for testing
    SLEEP_BEFORE_JOINT_CONSENSUE = 5
    SLEEP_BETWEEN_JOINT_CONSENSUS = 0
    SEND_TO_SERVER_DELAY = [0,0,0,0,0,0,0,0,0,0,0]
    SEND_TO_SERVER_LOST = [0,0,0,0,0,0,0,0,0,0,0]
    VERBOSE = True
