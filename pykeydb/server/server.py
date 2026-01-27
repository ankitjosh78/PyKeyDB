import asyncio
from pykeydb.db.pyKeyDB import get_pykey_db
from pykeydb.server.clientContext import ClientContext

HOST = "127.0.0.1"
PORT = 6379


db = get_pykey_db()


async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):

    addr = writer.get_extra_info("peername")
    client_context = ClientContext(db)
    print(f"Client connected: {addr}")
    print(f"Client context initialized for {addr}")

    try:
        while True:
            data = await reader.readline()
            if not data:
                break

            command = data.decode().strip().split()
            if not command:
                continue

            response = client_context.execute_command(command)
            writer.write((response + "\n").encode())
            await writer.drain()

    except Exception as e:
        print(f"Client error {addr}: {e}")

    finally:
        writer.close()
        await writer.wait_closed()
        print(f"Client disconnected: {addr}")


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
