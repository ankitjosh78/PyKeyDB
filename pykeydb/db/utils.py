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

        # Hash operations
        if op == "HSET" and len(cmd) >= 4 and len(cmd) % 2 == 0:
            key = cmd[1]
            values = cmd[2:]
            hash_dict = {}
            for i in range(0, len(values), 2):
                hash_dict[values[i]] = values[i + 1]
            length = db.hset(key, hash_dict)
            return f"(integer) {length}"

        if op == "HGET" and len(cmd) == 3:
            key = cmd[1]
            hkey = cmd[2]
            value = db.hget(key, hkey)

            return str(value) if value is not None else "(nil)"

        if op == "HMGET" and len(cmd) >= 3:
            key = cmd[1]
            hkeys = cmd[2:]
            values = db.hmget(key, *hkeys)

            return "\n".join(
                f"{i}) {value if value is not None else '(nil)'}"
                for i, value in enumerate(values, 1)
            )

        if op == "HGETALL" and len(cmd) == 2:
            key = cmd[1]
            hash_dict = db.hgetall(key)
            if not hash_dict:
                return "(empty hash)"

            return "\n".join(
                f"{i}) {k}: {v}" for i, (k, v) in enumerate(hash_dict.items(), 1)
            )

        if op == "HDEL" and len(cmd) >= 3:
            key = cmd[1]
            fields = cmd[2:]
            count = db.hdel(key, *fields)
            return f"(integer) {count}"

        if op == "HLEN" and len(cmd) == 2:
            key = cmd[1]
            count = db.hlen(key)
            return f"(integer) {count}"

        if op == "HEXISTS" and len(cmd) == 3:
            key = cmd[1]
            hkey = cmd[2]
            exists = db.hexists(key, hkey)
            return "(integer) 1" if exists else "(integer) 0"

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
