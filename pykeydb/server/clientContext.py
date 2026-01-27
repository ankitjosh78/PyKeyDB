from collections import deque
from pykeydb.db.utils import apply_command


class ClientContext:
    def __init__(self, db):
        self.in_txn: bool = False
        self.txn_queue = deque()
        self.db = db

    def execute_command(self, command):
        op = command[0].upper()
        # If client wants to begin a transcation block
        if op == "MULTI":
            if self.in_txn:
                return "ERR: Cannot be in a Nested Transaction State"
            # Clear any previous stale transaction state
            self.in_txn = True
            self.txn_queue.clear()
            return "OK"

        # If client wants to execute the transaction (all the previous commands will be executed as one atomic operation)
        elif op == "EXEC":
            if not self.in_txn:
                return "ERR: Not in Transaction Mode for EXEC"

            responses = []
            while self.txn_queue:
                cmd = self.txn_queue.popleft()
                response = apply_command(self.db, cmd)
                responses.append(response)

            self.in_txn = False
            self.txn_queue.clear()
            return "\n".join(responses)

        # If client wants to discard changes/quit transaction mode midway.
        elif op == "DISCARD":
            if self.in_txn:
                self.in_txn = False
                self.txn_queue.clear()
                return "OK"
            else:
                return "ERR: Not in Transaction Mode for DISCARD"

        # If already in transaction mode, queue the commands
        if self.in_txn:
            self.txn_queue.append(command)
            return "QUEUED"

        # If not in transction mode, just apply the commands
        response = apply_command(self.db, command)
        return str(response)
