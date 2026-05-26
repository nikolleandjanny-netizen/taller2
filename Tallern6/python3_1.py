#!/usr/bin/env python3
"""
Port Forwarder - Python 3 (asyncio)
Migrado desde Python 2.7 (asyncore) al estilo moderno con asyncio.

Uso:
    python port_forwarder.py --local-port 8888
    python port_forwarder.py --local-port 8888 --remote-host ejemplo.com --remote-port 80
"""

import argparse
import asyncio

# ─── Configuración por defecto ────────────────────────────────────────────────
LOCAL_SERVER_HOST  = 'localhost'
REMOTE_SERVER_HOST = 'www.google.com'
BUFSIZE = 4096


# ─── Protocolo de reenvío ─────────────────────────────────────────────────────

class ForwarderProtocol(asyncio.Protocol):
    """
    Protocolo genérico que conecta dos extremos (local ↔ remoto).
    Cuando recibe datos de un lado, los envía directamente al otro.
    """

    def __init__(self):
        self.peer: "ForwarderProtocol | None" = None
        self.transport: asyncio.Transport | None = None
        self._buffer: list[bytes] = []
        self._closed = False

    def connection_made(self, transport: asyncio.Transport):
        """Se llama cuando la conexión queda establecida."""
        self.transport = transport
        if self._buffer:
            for chunk in self._buffer:
                self.transport.write(chunk)
            self._buffer.clear()

    def data_received(self, data: bytes):
        """Se llama cada vez que llegan bytes. Los reenviamos al peer."""
        if self.peer and self.peer.transport:
            self.peer.transport.write(data)
        else:
            self._buffer.append(data)

    def connection_lost(self, exc):
        """Se llama cuando la conexión se cierra (por cualquier extremo)."""
        if self._closed:
            return
        self._closed = True
        if self.peer and not self.peer._closed:
            self.peer._closed = True
            if self.peer.transport:
                self.peer.transport.close()


# ─── Servidor de reenvío ──────────────────────────────────────────────────────

class PortForwarder:
    """
    Escucha conexiones entrantes en (local_host, local_port) y, por cada una,
    abre una conexión saliente hacia (remote_host, remote_port).
    """

    def __init__(self, local_host: str, local_port: int,
                 remote_host: str, remote_port: int):
        self.local_host  = local_host
        self.local_port  = local_port
        self.remote_host = remote_host
        self.remote_port = remote_port

    async def handle_client(self,
                             reader: asyncio.StreamReader,
                             writer: asyncio.StreamWriter):
        """
        Callback para cada cliente nuevo.
        Crea la conexión al remoto y lanza las dos tareas de copia en paralelo.
        """
        addr = writer.get_extra_info('peername')
        print(f"[+] Nueva conexión desde: {addr}")

        try:
            remote_reader, remote_writer = await asyncio.open_connection(
                self.remote_host, self.remote_port
            )
            print(f"[+] Conectado a remoto: {self.remote_host}:{self.remote_port}")
        except OSError as e:
            print(f"[-] No se pudo conectar al remoto: {e}")
            writer.close()
            return

        await asyncio.gather(
            self._pipe(reader, remote_writer, label="local→remoto"),
            self._pipe(remote_reader, writer,  label="remoto→local"),
            return_exceptions=True,
        )

        for w in (writer, remote_writer):
            try:
                w.close()
                await w.wait_closed()
            except Exception:
                pass

        print(f"[-] Conexión cerrada: {addr}")

    @staticmethod
    async def _pipe(reader: asyncio.StreamReader,
                    writer: asyncio.StreamWriter,
                    label: str = ""):
        """Lee de 'reader' en bloques de BUFSIZE y escribe en 'writer'."""
        try:
            while True:
                data = await reader.read(BUFSIZE)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except (ConnectionResetError, BrokenPipeError, asyncio.CancelledError):
            pass

    async def start(self):
        """Arranca el servidor y espera indefinidamente."""
        server = await asyncio.start_server(
            self.handle_client,
            self.local_host,
            self.local_port,
        )
        addrs = ', '.join(str(s.getsockname()) for s in server.sockets)
        print(f"[*] Escuchando en {addrs}")
        print(f"[*] Reenviando hacia {self.remote_host}:{self.remote_port}")

        async with server:
            await server.serve_forever()


# ─── Punto de entrada ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ejemplo de port forwarding')
    parser.add_argument('--local-host',  dest='local_host',
                        default=LOCAL_SERVER_HOST,
                        help='IP/host local donde escuchar')
    parser.add_argument('--local-port',  dest='local_port',
                        type=int, required=True,
                        help='Puerto local donde escuchar')
    parser.add_argument('--remote-host', dest='remote_host',
                        default=REMOTE_SERVER_HOST,
                        help='Host remoto al que reenviar')
    parser.add_argument('--remote-port', dest='remote_port',
                        type=int, default=80,
                        help='Puerto remoto al que reenviar')

    args = parser.parse_args()

    print(
        f"[*] Iniciando port forwarding: "
        f"{args.local_host}:{args.local_port} → "
        f"{args.remote_host}:{args.remote_port}"
    )

    forwarder = PortForwarder(
        args.local_host, args.local_port,
        args.remote_host, args.remote_port,
    )

    try:
        asyncio.run(forwarder.start())
    except KeyboardInterrupt:
        print("\n[*] Detenido por el usuario.")