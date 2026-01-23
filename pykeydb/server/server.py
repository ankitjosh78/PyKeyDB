import asyncio
from pykeydb.db.pyKeyDB import get_pykey_db

HOST = "127.0.0.1"
PORT = 6379


db = get_pykey_db()


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):

    addr = writer.get_extra_info("peername")
    print(f"Client connected: {addr}")

    try:
        while True:
            data = await reader.readline()
            if not data:
                break

            command = data.decode().strip().split()
            if not command:
                continue

            response = execute_command(command)
            writer.write((response + "\n").encode())
            await writer.drain()

    except Exception as e:
        print(f"Client error {addr}: {e}")

    finally:
        writer.close()
        await writer.wait_closed()
        print(f"Client disconnected: {addr}")


def execute_command(cmd: list[str]) -> str:
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


async def main():
    server = await asyncio.start_server(handle_client, HOST, PORT)
    print(f"PyKeyDB server listening on {HOST}:{PORT}")

    try:
        async with server:
            await server.serve_forever()
    except asyncio.CancelledError:
        print("\nShutting down PyKeyDB server...")


if __name__ == "__main__":
    asyncio.run(main())
