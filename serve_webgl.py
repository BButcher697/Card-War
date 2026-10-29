import argparse
import base64
import functools
import hashlib
import gzip
import http
import http.server
import json
import os
import pathlib
import urllib.parse
import random
import socket
import socketserver
import struct
import threading


WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class Room:
    def __init__(self, code, host_handler, host_name="Host"):
        self.code = code
        self.host = host_handler
        self.guest = None
        self.seat_ws = {0: host_handler, 1: None}
        self.names = {0: host_name, 1: None}
        self.current_seat = 0
        self.seed = None
        self.matched = False

    def get_seat(self, handler):
        if handler == self.seat_ws.get(0):
            return 0
        if handler == self.seat_ws.get(1):
            return 1
        return None

    def broadcast(self, payload, send_fn):
        for s in (0, 1):
            h = self.seat_ws.get(s)
            if h:
                send_fn(h, payload)


class Relay:
    """Multi-room WebSocket relay for Card Wars.
    Supports private room codes (Create / Join) and Quick Match.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.clients = {}         # handler -> {"name": str, "room_code": str|None}
        self.rooms = {}           # code -> Room
        self.quick_waiting = None # handler waiting for quick match

    def register(self, handler):
        with self.lock:
            self.clients[handler] = {"name": "Player", "room_code": None}

    def unregister(self, handler):
        with self.lock:
            info = self.clients.pop(handler, None)
            if handler == self.quick_waiting:
                self.quick_waiting = None

            if info and info.get("room_code"):
                code = info["room_code"]
                room = self.rooms.get(code)
                if room:
                    close_msg = json.dumps({"t": "close"})
                    room.broadcast(close_msg, self._send_locked)
                    other = room.guest if handler == room.host else room.host
                    if other and other in self.clients:
                        self.clients[other]["room_code"] = None
                    self.rooms.pop(code, None)

    def _generate_code(self):
        for _ in range(100):
            code = str(random.randint(1000, 9999))
            if code not in self.rooms:
                return code
        return str(random.randint(10000, 99999))

    def on_message(self, handler, text):
        try:
            msg = json.loads(text)
        except ValueError:
            return
        t = msg.get("t")

        with self.lock:
            info = self.clients.get(handler)
            if not info:
                return

        # 1. CREATE PRIVATE ROOM
        if t == "create_room":
            name = str(msg.get("n", "Host"))[:24]
            with self.lock:
                code = self._generate_code()
                room = Room(code, handler, name)
                self.rooms[code] = room
                info["name"] = name
                info["room_code"] = code
                payload = json.dumps({"t": "room_created", "code": code})
                self._send_locked(handler, payload)
            return

        # 2. JOIN PRIVATE ROOM
        if t == "join_room":
            code = str(msg.get("code", "")).strip().upper()
            name = str(msg.get("n", "Guest"))[:24]
            with self.lock:
                room = self.rooms.get(code)
                if not room:
                    self._send_locked(handler, json.dumps({"t": "room_error", "m": f"Room {code} not found."}))
                    return
                if room.guest is not None or room.matched:
                    self._send_locked(handler, json.dumps({"t": "room_error", "m": f"Room {code} is full."}))
                    return
                room.guest = handler
                room.seat_ws[1] = handler
                room.names[1] = name
                room.matched = True
                room.seed = random.randint(1, 2147483647)
                room.current_seat = 0
                info["name"] = name
                info["room_code"] = code

                host_payload = json.dumps({"t": "matched", "seed": room.seed, "s": 0, "opp": name, "code": code})
                guest_payload = json.dumps({"t": "matched", "seed": room.seed, "s": 1, "opp": room.names[0], "code": code})
                self._send_locked(room.host, host_payload)
                self._send_locked(room.guest, guest_payload)
            return

        # 3. CANCEL ROOM
        if t == "cancel_room":
            with self.lock:
                code = info.get("room_code")
                if code and code in self.rooms:
                    room = self.rooms.pop(code)
                    info["room_code"] = None
                    close_msg = json.dumps({"t": "close"})
                    room.broadcast(close_msg, self._send_locked)
            return

        # 4. QUICK MATCH (Random 1v1)
        if t in ("join", "quick_match"):
            name = str(msg.get("n", "Player"))[:24]
            with self.lock:
                info["name"] = name
                if self.quick_waiting is None or self.quick_waiting == handler:
                    self.quick_waiting = handler
                    code = self._generate_code()
                    room = Room(code, handler, name)
                    self.rooms[code] = room
                    info["room_code"] = code
                    self._send_locked(handler, json.dumps({"t": "joined", "s": 0, "n": name}))
                else:
                    host_h = self.quick_waiting
                    self.quick_waiting = None
                    host_info = self.clients.get(host_h)
                    code = host_info.get("room_code") if host_info else None
                    room = self.rooms.get(code) if code else None
                    if not room:
                        code = self._generate_code()
                        room = Room(code, host_h, host_info.get("name", "Host") if host_info else "Host")
                        self.rooms[code] = room
                        if host_info:
                            host_info["room_code"] = code

                    room.guest = handler
                    room.seat_ws[1] = handler
                    room.names[1] = name
                    room.matched = True
                    room.seed = random.randint(1, 2147483647)
                    room.current_seat = 0
                    info["room_code"] = code

                    host_payload = json.dumps({"t": "matched", "seed": room.seed, "s": 0, "opp": name, "code": code})
                    guest_payload = json.dumps({"t": "matched", "seed": room.seed, "s": 1, "opp": room.names[0], "code": code})
                    self._send_locked(room.host, host_payload)
                    self._send_locked(room.guest, guest_payload)
            return

        # 5. IN-GAME ACTIONS (move, win, chat)
        with self.lock:
            code = info.get("room_code")
            room = self.rooms.get(code)
            if not room:
                return
            seat = room.get_seat(handler)
            if seat is None:
                return

        if t == "move":
            with self.lock:
                next_seat = msg.get("ns", 1 - seat)
                if next_seat not in (0, 1):
                    next_seat = 1 - seat
                room.current_seat = next_seat
                out = dict(msg)
                out["t"] = "move"
                out["s"] = seat
                out["ns"] = next_seat
                payload = json.dumps(out)
                room.broadcast(payload, self._send_locked)
            return

        if t == "win":
            with self.lock:
                out = json.dumps({"t": "win", "s": seat})
                room.broadcast(out, self._send_locked)
            return

        if t == "chat":
            text_msg = str(msg.get("m", ""))[:120]
            with self.lock:
                out = json.dumps({"t": "chat", "s": seat, "n": info.get("name"), "m": text_msg})
                room.broadcast(out, self._send_locked)
            return

    def _send_locked(self, handler, payload):
        if not isinstance(payload, str):
            payload = json.dumps(payload)
        try:
            handler.send_frame(payload)
        except OSError:
            pass

    def _broadcast_locked(self, obj):
        payload = json.dumps(obj)
        for h in list(self.clients.keys()):
            self._send_locked(h, payload)

RELAY = Relay()


class GameHandler(http.server.SimpleHTTPRequestHandler):
    extensions_map = {
        **http.server.SimpleHTTPRequestHandler.extensions_map,
        ".wasm": "application/wasm",
        ".data": "application/octet-stream",
        ".unityweb": "application/octet-stream",
        ".mem": "application/octet-stream",
        ".js": "application/javascript",
        ".json": "application/json",
        ".symbols.json": "application/json",
        ".html": "text/html; charset=utf-8",
        ".css": "text/css",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".svg": "image/svg+xml",
        ".ico": "image/x-icon",
    }

    def send_head(self):
        path = self.translate_path(self.path)
        if os.path.isdir(path):
            parts = urllib.parse.urlsplit(self.path)
            if not parts.path.endswith('/'):
                self.send_response(http.HTTPStatus.MOVED_PERMANENTLY)
                new_parts = (parts[0], parts[1], parts[2] + '/', parts[3], parts[4])
                new_url = urllib.parse.urlunsplit(new_parts)
                self.send_header("Location", new_url)
                self.end_headers()
                return None
            for index in "index.html", "index.htm":
                index = os.path.join(path, index)
                if os.path.exists(index):
                    path = index
                    break
            else:
                return self.list_directory(path)

        ctype = self.guess_type(path)
        accept_encoding = self.headers.get("Accept-Encoding", "")
        gz_path = path + ".gz"
        use_gzip = ("gzip" in accept_encoding) and os.path.exists(gz_path) and not path.endswith(".gz")
        serve_path = gz_path if use_gzip else path

        try:
            f = open(serve_path, 'rb')
        except OSError:
            self.send_error(http.HTTPStatus.NOT_FOUND, "File not found")
            return None

        try:
            fs = os.fstat(f.fileno())
            self.send_response(http.HTTPStatus.OK)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(fs[6]))
            self.send_header("Last-Modified", self.date_time_string(fs.st_mtime))
            if use_gzip:
                self.send_header("Content-Encoding", "gzip")

            if path.endswith(".html") or path.endswith("/") or path.endswith("\\"):
                self.send_header("Cache-Control", "no-cache, must-revalidate")
            else:
                self.send_header("Cache-Control", "public, max-age=604800, immutable")

            http.server.SimpleHTTPRequestHandler.end_headers(self)
            return f
        except Exception:
            f.close()
            raise

    def log_message(self, fmt, *args):
        if "/ws" not in (self.path or ""):
            print("%s - %s" % (self.address_string(), fmt % args), flush=True)

    def do_GET(self):
        parsed = self.path
        if "?" in parsed:
            parsed = parsed.split("?", 1)[0]
        if parsed == "/ws":
            self.handle_websocket()
            return
        super().do_GET()

    # ---- WebSocket upgrade + relay (same port, works through HTTP tunnels) ----
    def handle_websocket(self):
        if (self.headers.get("Upgrade", "") or "").lower() != "websocket":
            self.send_error(400, "Expected WebSocket upgrade")
            return
        key = self.headers.get("Sec-WebSocket-Key")
        if not key:
            self.send_error(400, "Missing Sec-WebSocket-Key")
            return
        accept = base64.b64encode(
            hashlib.sha1((key + WS_GUID).encode()).digest()
        ).decode()
        response = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
        ).encode("utf-8")
        try:
            self.wfile.write(response)
            self.wfile.flush()
        except OSError:
            return

        RELAY.register(self)
        conn = self.connection
        try:
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass
        try:
            conn.settimeout(120.0)
            while True:
                frame = self.recv_frame(conn)
                if frame is None:
                    break
                opcode, payload = frame
                if opcode == 0x8:  # close
                    break
                if opcode == 0x1:  # text
                    try:
                        text = payload.decode("utf-8")
                    except UnicodeDecodeError:
                        continue
                    RELAY.on_message(self, text)
                # ping/pong and other opcodes are ignored
        except (OSError, socket.timeout):
            pass
        finally:
            try:
                conn.settimeout(None)
            except OSError:
                pass
            RELAY.unregister(self)

    def recv_frame(self, conn):
        header = self.recv_exact(conn, 2)
        if not header or len(header) < 2:
            return None
        b1, b2 = header
        opcode = b1 & 0x0F
        masked = bool(b2 & 0x80)
        length = b2 & 0x7F
        if length == 126:
            length = struct.unpack(">H", self.recv_exact(conn, 2))[0]
        elif length == 127:
            length = struct.unpack(">Q", self.recv_exact(conn, 8))[0]
        mask = self.recv_exact(conn, 4) if masked else None
        payload = self.recv_exact(conn, length) if length else b""
        if payload is None:
            return None
        if mask:
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        return opcode, payload

    def recv_exact(self, conn, n):
        chunks = []
        remaining = n
        while remaining > 0:
            try:
                chunk = conn.recv(remaining)
            except socket.timeout:
                return None
            if not chunk:
                return None
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def send_frame(self, text):
        payload = text.encode("utf-8")
        n = len(payload)
        b1 = 0x81  # FIN + text frame
        if n < 126:
            header = bytes([b1, n])
        elif n <= 65535:
            header = bytes([b1, 126]) + struct.pack(">H", n)
        else:
            header = bytes([b1, 127]) + struct.pack(">Q", n)
        try:
            self.connection.sendall(header + payload)
        except OSError:
            pass


def prepare_gzip_assets(root_path):
    root = pathlib.Path(root_path)
    for ext in (".wasm", ".data", ".framework.js"):
        for p in root.rglob("*" + ext):
            gz = p.with_name(p.name + ".gz")
            if not gz.exists() or gz.stat().st_mtime < p.stat().st_mtime:
                try:
                    with open(p, "rb") as f_in, gzip.open(gz, "wb", compresslevel=6) as f_out:
                        f_out.write(f_in.read())
                    sz_orig = p.stat().st_size / (1024 * 1024)
                    sz_gz = gz.stat().st_size / (1024 * 1024)
                    print(f"  [gzip] Compressed {p.name} ({sz_orig:.1f}MB -> {sz_gz:.1f}MB)", flush=True)
                except Exception as err:
                    print(f"  [gzip] Error compressing {p.name}: {err}", flush=True)

def local_ip():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        ip = sock.getsockname()[0]
        sock.close()
        return ip
    except OSError:
        return "127.0.0.1"


def default_root():
    """Support both layouts: project (Build/WebGL) and deploy repo (index.html at root)."""
    here = os.path.dirname(os.path.abspath(__file__))
    nested = os.path.join(here, "Build", "WebGL")
    if os.path.isfile(os.path.join(nested, "index.html")):
        return nested
    return here


def main():
    parser = argparse.ArgumentParser(description="Serve the Card Wars WebGL build + online relay.")
    # Render (and most PaaS hosts) inject the listening port through $PORT.
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8080")))
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--root", default=default_root())
    args = parser.parse_args()

    root = os.path.abspath(args.root)
    index = os.path.join(root, "index.html")
    if not os.path.isfile(index):
        raise SystemExit("Build not found: " + index)

    prepare_gzip_assets(root)

    socketserver.ThreadingTCPServer.allow_reuse_address = True
    handler = functools.partial(GameHandler, directory=root)

    with socketserver.ThreadingTCPServer((args.host, args.port), handler) as httpd:
        httpd.daemon_threads = True
        print("Card Wars WebGL server + online relay")
        print("  HTTP/WebSocket: http://127.0.0.1:%d/  (ws on /ws)" % args.port)
        print("  Phone (same Wi-Fi): http://%s:%d/  (ws on /ws)" % (local_ip(), args.port))
        print("  Serving: " + root)
        print("Press Ctrl+C to stop.", flush=True)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nServer stopped.", flush=True)


if __name__ == "__main__":
    main()
