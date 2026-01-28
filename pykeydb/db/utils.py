def apply_command(db, cmd: list[str]) -> str:
    try:
        op = cmd[0].upper()

        # String operations
        if op == "SET" and len(cmd) >= 3:
            key = cmd[1]
            value = " ".join(cmd[2:])
            return "OK" if db.set(key, value) else "ERR"

        if op == "GET" and len(cmd) == 2:
            val = db.get(cmd[1])
            return str(val) if val is not None else "(nil)"

        # List operations
        if op == "LPUSH" and len(cmd) >= 3:
            key = cmd[1]
            values = cmd[2:]
            length = db.lpush(key, *values)
            return f"(integer) {length}"

        if op == "RPUSH" and len(cmd) >= 3:
            key = cmd[1]
            values = cmd[2:]
            length = db.rpush(key, *values)
            return f"(integer) {length}"

        if op == "LPOP" and len(cmd) == 2:
            val = db.lpop(cmd[1])
            return str(val) if val is not None else "(nil)"

        if op == "RPOP" and len(cmd) == 2:
            val = db.rpop(cmd[1])
            return str(val) if val is not None else "(nil)"

        if op == "LRANGE" and len(cmd) == 4:
            key = cmd[1]
            start = int(cmd[2])
            stop = int(cmd[3])
            items = db.lrange(key, start, stop)
            if not items:
                return "(EMPTY LIST)"
            return "\n".join(f"{i}) {v}" for i, v in enumerate(items, 1))

        if op == "LLEN" and len(cmd) == 2:
            length = db.llen(cmd[1])
            return f"(integer) {length}"

        # General operations
        if op == "DEL" and len(cmd) == 2:
            return "OK" if db.delete(cmd[1]) else "NULL"

        if op == "TYPE" and len(cmd) == 2:
            t = db.type(cmd[1])
            return t if t else "NULL"

        return "ERR unknown command"

    except TypeError as e:
        return f"ERR {e}"
    except ValueError as e:
        return f"ERR invalid argument: {e}"
    except Exception as e:
        return f"ERR {e}"
