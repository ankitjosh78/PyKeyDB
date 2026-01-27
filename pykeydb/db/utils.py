def apply_command(db, cmd: list[str]) -> str:
    try:
        op = cmd[0].upper()

        if op == "SET" and len(cmd) >= 3:
            key = cmd[1]
            value = " ".join(cmd[2:])
            return "OK" if db.set(key, value) else "ERR"

        if op == "GET" and len(cmd) == 2:
            val = db.get(cmd[1])
            return str(val) if val is not None else "NULL"

        if op == "DEL" and len(cmd) == 2:
            return "OK" if db.delete(cmd[1]) else "NULL"

        if op == "TYPE" and len(cmd) == 2:
            t = db.type(cmd[1])
            return t if t else "NULL"

        return "ERR unknown command"

    except Exception as e:
        return f"ERR {e}"
