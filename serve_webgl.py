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
import time


WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
LOBBY_SECONDS = 300
MAX_PLAYERS = 6


class Room:
    """A private room lobby: host (seat 0) + up to 5 joiners, then a shared game."""

    def __init__(self, code, host_handler, host_name="Host", mode="Classic", capacity=4):
        self.code = code
        self.mode = mode
        self.capacity = capacity
        self.members = {0: host_handler}  # seat -> handler (None if temporarily disconnected)
        self.names = {0: host_name}       # seat -> display name
        self.started = False
        self.seed = None
        self.deadline = time.time() + LOBBY_SECONDS  # None once held (solo past timer)
        self.current_seat = 0
        self.all_seats = [0]  # seat snapshot taken at game start (survives mid-game leaves)
        self.turn_deadline = None
        self.finished_seats = set()
        self.host_seat = 0
        self.disconnected = {}  # seat -> {"deadline": float, "name": str}

    def next_seat(self, from_seat):
        fin = getattr(self, "finished_seats", set())
        valid = [s for s in sorted(self.members.keys()) if s not in fin]
        if not valid:
            return 0
        for s in valid:
            if s > from_seat:
                return s
        return valid[0]

    def free_seat(self):
        cap = getattr(self, "capacity", MAX_PLAYERS)
        for s in range(cap):
            if s not in self.members:
                return s
        return None

    def get_seat(self, handler):
        for s, h in self.members.items():
            if h is not None and h == handler:
                return s
        return None

    def remove(self, handler):
        seat = self.get_seat(handler)
        if seat is None:
            return None
        self.members.pop(seat, None)
        self.names.pop(seat, None)
        if hasattr(self, "disconnected"):
            self.disconnected.pop(seat, None)
        if hasattr(self, "finished_seats"):
            self.finished_seats.discard(seat)
        if seat == getattr(self, "host_seat", 0) and self.members:
            valid_seats = [s for s, h in self.members.items() if h is not None]
            if valid_seats:
                self.host_seat = min(valid_seats)
            elif self.members:
                self.host_seat = min(self.members.keys())
        return seat

    def players_csv(self):
        return ",".join(self.names.get(s, "") for s in sorted(self.names.keys()))

    def broadcast(self, payload, send_fn):
        for h in list(self.members.values()):
            if h is not None:
                send_fn(h, payload)


class Relay:
    """Multi-room WebSocket relay for Card Wars.
    Private rooms: create -> share 4-digit code -> lobby (5 min countdown,
    up to 6 players) -> host starts early or the timer starts the game.
    Quick Match: instant random 1v1 pairing.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.clients = {}         # handler -> {"name": str, "room_code": str|None}
        self.rooms = {}           # code -> Room
        self.quick_waiting = None # handler waiting for quick match
        self._timer_thread = threading.Thread(target=self._timer_loop, daemon=True)
        self._timer_thread.start()

    def _timer_loop(self):
        while True:
            time.sleep(0.5)
            with self.lock:
                for room in list(self.rooms.values()):
                    if not room.started:
                        if room.deadline is None:
                            continue
                        if time.time() < room.deadline:
                            continue
                        if len(room.members) >= 2:
                            self._start_room(room)
                        else:
                            # Held: no one else arrived. Host can still start when a friend joins.
                            room.deadline = None
                            self._broadcast_lobby(room)
                    else:
                        # 1. In-game disconnect grace period check (25-second reconnect window)
                        if hasattr(room, "disconnected") and room.disconnected:
                            now = time.time()
                            for d_seat in list(room.disconnected.keys()):
                                d_info = room.disconnected[d_seat]
                                if now >= d_info["deadline"]:
                                    # 25 seconds grace expired! Finalize forfeit
                                    room.disconnected.pop(d_seat, None)
                                    room.members.pop(d_seat, None)
                                    d_name = room.names.pop(d_seat, d_info.get("name", "Player"))
                                    if hasattr(room, "finished_seats"):
                                        room.finished_seats.discard(d_seat)

                                    room.broadcast(json.dumps({
                                        "t": "player_left",
                                        "s": d_seat,
                                        "n": d_name,
                                        "timeout": True
                                    }), self._send_locked)

                                    fin = getattr(room, "finished_seats", set())
                                    active = [s for s, h in room.members.items() if h is not None and s not in fin]
                                    if len(active) <= 1:
                                        if active:
                                            room.broadcast(json.dumps({"t": "win", "s": active[0]}), self._send_locked)

                                    if not room.members or not any(h is not None for h in room.members.values()):
                                        self.rooms.pop(room.code, None)
                                    elif room.current_seat == d_seat:
                                        next_s = room.next_seat(d_seat)
                                        room.current_seat = next_s
                                        r_mode = getattr(room, "mode", "Classic")
                                        is_blitz = (r_mode.lower() == "blitz" or r_mode == "Blitz Mode")
                                        room.turn_deadline = time.time() + (11.0 if is_blitz else 18.0)
                                        auto_pass = {
                                            "t": "move",
                                            "s": d_seat,
                                            "k": "pass",
                                            "ns": next_s,
                                            "id": -98,
                                            "left": True,
                                        }
                                        room.broadcast(json.dumps(auto_pass), self._send_locked)

                        # 2. In-game turn timeout check (Classic: 15s + 3s grace = 18s; Blitz: 8s + 3s grace = 11s)
                        if room.turn_deadline is not None and time.time() >= room.turn_deadline:
                            # Do not auto-pass turn if someone in the room is actively in 25s reconnect grace!
                            if hasattr(room, "disconnected") and room.disconnected:
                                continue
                            fin = getattr(room, "finished_seats", set())
                            active = [s for s, h in room.members.items() if h is not None and s not in fin]
                            if len(active) >= 2:
                                cur_seat = room.current_seat
                                next_s = room.next_seat(cur_seat)
                                room.current_seat = next_s
                                r_mode = getattr(room, "mode", "Classic")
                                is_blitz = (r_mode.lower() == "blitz" or r_mode == "Blitz Mode")
                                room.turn_deadline = time.time() + (11.0 if is_blitz else 18.0)
                                auto_pass = {
                                    "t": "move",
                                    "s": cur_seat,
                                    "k": "pass",
                                    "ns": next_s,
                                    "id": -99,
                                    "timeout": True,
                                }
                                room.broadcast(json.dumps(auto_pass), self._send_locked)

    def _lobby_payload(self, room):
        left = 0
        if room.deadline is not None:
            left = max(0, int(room.deadline - time.time()))
        return json.dumps({
            "t": "lobby",
            "code": room.code,
            "players": room.players_csv(),
            "cnt": len(room.members),
            "left": left,
            "host": getattr(room, "host_seat", 0),
        })

    def _broadcast_lobby(self, room):
        left = 0
        if room.deadline is not None:
            left = max(0, int(room.deadline - time.time()))
        host_s = getattr(room, "host_seat", 0)
        csv = room.players_csv()
        cnt = len(room.members)
        r_mode = getattr(room, "mode", "Classic")
        r_cap = getattr(room, "capacity", 4)
        for seat, handler in list(room.members.items()):
            payload = json.dumps({
                "t": "lobby",
                "code": room.code,
                "players": csv,
                "cnt": cnt,
                "cap": r_cap,
                "left": left,
                "host": host_s,
                "s": seat,
                "mode": r_mode,
            })
            self._send_locked(handler, payload)

    def _start_room(self, room):
        room.started = True
        room.deadline = None
        room.finished_seats = set()
        room.all_seats = sorted(room.members.keys())
        room.seed = random.randint(1, 2147483647)
        active_seats = sorted(room.members.keys())
        room.current_seat = random.choice(active_seats) if active_seats else 0
        r_mode = getattr(room, "mode", "Classic")
        is_blitz = (r_mode.lower() == "blitz" or r_mode == "Blitz Mode")
        room.turn_deadline = time.time() + (11.0 if is_blitz else 18.0)
        players = room.players_csv()
        for seat, handler in list(room.members.items()):
            payload = json.dumps({
                "t": "start",
                "seed": room.seed,
                "s": seat,
                "lead": room.current_seat,
                "players": players,
                "code": room.code,
                "mode": r_mode,
            })
            self._send_locked(handler, payload)

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
                    if not room.started:
                        seat = room.remove(handler)
                        if seat == 0:
                            # Host left before the game: close the room for everyone.
                            close_msg = json.dumps({"t": "close"})
                            room.broadcast(close_msg, self._send_locked)
                            for other in list(room.members.values()):
                                if other in self.clients:
                                    self.clients[other]["room_code"] = None
                            self.rooms.pop(code, None)
                        elif seat is not None:
                            self._broadcast_lobby(room)
                    else:
                        # Mid-game disconnect: do NOT immediately forfeit!
                        # Mark seat as disconnected and give a 25-second grace period for reconnection.
                        seat = room.get_seat(handler)
                        if seat is not None:
                            name = room.names.get(seat, "Player")
                            room.members[seat] = None
                            if not hasattr(room, "disconnected"):
                                room.disconnected = {}
                            deadline = time.time() + 25.0
                            room.disconnected[seat] = {
                                "deadline": deadline,
                                "name": name
                            }
                            disconnect_msg = json.dumps({
                                "t": "player_disconnected",
                                "s": seat,
                                "n": name,
                                "grace": 25,
                                "cur": room.current_seat
                            })
                            room.broadcast(disconnect_msg, self._send_locked)

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

        # Instant Ping / Pong for latency measurement
        if t == "ping":
            resp = {"t": "pong"}
            if "ts" in msg:
                resp["ts"] = msg["ts"]
            if "id" in msg:
                resp["id"] = msg["id"]
            self._send_locked(handler, json.dumps(resp))
            return

        with self.lock:
            info = self.clients.get(handler)
            if not info:
                return

        # 1. CREATE PRIVATE ROOM
        if t == "create_room":
            name = str(msg.get("n", "Host"))[:24]
            mode = str(msg.get("mode", "Classic"))[:32]
            try:
                cap = int(msg.get("cap", msg.get("cnt", 4)))
            except Exception:
                cap = 4
            if cap not in (2, 3, 4, 5, 6):
                cap = 4
            if len(name.strip()) < 3:
                self._send_locked(handler, json.dumps({"t": "room_error", "m": "Name needs at least 3 characters."}))
                return
            with self.lock:
                code = self._generate_code()
                room = Room(code, handler, name, mode=mode, capacity=cap)
                self.rooms[code] = room
                info["name"] = name
                info["room_code"] = code
                payload = json.dumps({"t": "room_created", "code": code, "s": 0, "host": 0, "mode": mode, "cap": cap})
                self._send_locked(handler, payload)
                self._broadcast_lobby(room)
            return

        # 2. JOIN PRIVATE ROOM
        if t == "join_room":
            code = str(msg.get("code", "")).strip().upper()
            name = str(msg.get("n", "Guest"))[:24]
            if len(name.strip()) < 3:
                self._send_locked(handler, json.dumps({"t": "room_error", "m": "Name needs at least 3 characters."}))
                return
            with self.lock:
                room = self.rooms.get(code)
                if not room:
                    self._send_locked(handler, json.dumps({"t": "room_error", "m": f"Room {code} not found."}))
                    return
                if room.started:
                    self._send_locked(handler, json.dumps({"t": "room_error", "m": f"Room {code} already started."}))
                    return
                seat = room.free_seat()
                if seat is None:
                    self._send_locked(handler, json.dumps({"t": "room_error", "m": f"Room {code} is full ({getattr(room, 'capacity', 4)} players)."}))
                    return
                room.members[seat] = handler
                room.names[seat] = name
                info["name"] = name
                info["room_code"] = code
                self._send_locked(handler, json.dumps({
                    "t": "room_joined",
                    "code": code,
                    "s": seat,
                    "host": getattr(room, "host_seat", 0),
                    "mode": getattr(room, "mode", "Classic"),
                    "cap": getattr(room, "capacity", 4),
                }))
                self._broadcast_lobby(room)
                # If the lobby timer already expired while the host waited alone, start now.
                if len(room.members) >= 2 and room.deadline is not None and time.time() >= room.deadline:
                    self._start_room(room)
            return

        # 3. HOST STARTS THE GAME EARLY
        if t == "start_game":
            with self.lock:
                code = info.get("room_code")
                room = self.rooms.get(code) if code else None
                if not room or room.started:
                    return
                if room.get_seat(handler) != getattr(room, "host_seat", 0):
                    return
                if len(room.members) < 2:
                    self._send_locked(handler, json.dumps({"t": "room_error", "m": "Need at least 2 players to start."}))
                    return
                self._start_room(room)
            return

        # 4. CANCEL ROOM
        if t == "cancel_room":
            with self.lock:
                code = info.get("room_code")
                room = self.rooms.get(code) if code else None
                if room and room.get_seat(handler) == getattr(room, "host_seat", 0):
                    self.rooms.pop(code, None)
                    info["room_code"] = None
                    close_msg = json.dumps({"t": "close"})
                    room.broadcast(close_msg, self._send_locked)
                    for other in list(room.members.values()):
                        if other in self.clients:
                            self.clients[other]["room_code"] = None
            return

        # 4b. PLAY AGAIN IN SAME ROOM
        if t == "play_again":
            with self.lock:
                code = info.get("room_code")
                room = self.rooms.get(code) if code else None
                if not room:
                    return
                seat = room.get_seat(handler)
                if seat is None:
                    return

                # Reset room back to lobby mode with 5 minute timer
                if room.started:
                    room.started = False
                    if hasattr(room, "finished_seats"):
                        room.finished_seats.clear()
                    room.deadline = time.time() + LOBBY_SECONDS
                    room.turn_deadline = None

                self._broadcast_lobby(room)
            return

        # 4c. LEAVE ROOM (Lobby or In-Game intentional leave)
        if t in ("leave_room", "leave"):
            with self.lock:
                code = info.get("room_code")
                room = self.rooms.get(code) if code else None
                if room:
                    seat = room.remove(handler)
                    info["room_code"] = None
                    if seat is not None:
                        if hasattr(room, "disconnected"):
                            room.disconnected.pop(seat, None)
                        room.broadcast(json.dumps({"t": "player_left", "s": seat, "intentional": True}), self._send_locked)
                        if not room.started:
                            self._broadcast_lobby(room)
                        else:
                            fin = getattr(room, "finished_seats", set())
                            active = [s for s, h in room.members.items() if h is not None and s not in fin]
                            if len(active) <= 1:
                                if active:
                                    room.broadcast(json.dumps({"t": "win", "s": active[0]}), self._send_locked)
                    if not room.members or not any(h is not None for h in room.members.values()):
                        self.rooms.pop(code, None)
                self._send_locked(handler, json.dumps({"t": "left_room"}))
            return

        # 4d. RECONNECT TO ACTIVE GAME (within 25s grace period)
        if t == "reconnect":
            code = str(msg.get("code", "")).strip().upper()
            seat = msg.get("s")
            name = str(msg.get("n", "Player"))[:24]
            with self.lock:
                room = self.rooms.get(code)
                if not room or not room.started:
                    self._send_locked(handler, json.dumps({
                        "t": "reconnect_failed",
                        "m": "Game is no longer active."
                    }))
                    return

                all_s = getattr(room, "all_seats", list(room.members.keys()))
                if not isinstance(seat, int) or (seat not in all_s and seat not in room.members):
                    self._send_locked(handler, json.dumps({
                        "t": "reconnect_failed",
                        "m": "Invalid seat for this match."
                    }))
                    return

                if hasattr(room, "disconnected") and seat in room.disconnected:
                    room.disconnected.pop(seat, None)

                room.members[seat] = handler
                room.names[seat] = name
                info["name"] = name
                info["room_code"] = code

                r_mode = getattr(room, "mode", "Classic")
                players = room.players_csv()
                turn_left = 0
                if room.turn_deadline is not None:
                    turn_left = max(1, int(room.turn_deadline - time.time()))

                self._send_locked(handler, json.dumps({
                    "t": "reconnected",
                    "code": room.code,
                    "s": seat,
                    "cur": room.current_seat,
                    "lead": room.current_seat,
                    "players": players,
                    "mode": r_mode,
                    "left": turn_left
                }))

                reconnected_notice = json.dumps({
                    "t": "player_reconnected",
                    "s": seat,
                    "n": name,
                    "cur": room.current_seat
                })
                for s, h in room.members.items():
                    if s != seat and h is not None:
                        self._send_locked(h, reconnected_notice)
            return

        # 5. QUICK MATCH (Random 1v1)
        if t in ("join", "quick_match"):
            name = str(msg.get("n", "Player"))[:24]
            if len(name.strip()) < 3:
                self._send_locked(handler, json.dumps({"t": "room_error", "m": "Name needs at least 3 characters."}))
                return
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

                    room.members[1] = handler
                    room.names[1] = name
                    info["room_code"] = code
                    self._start_room(room)
            return

        # 6. IN-GAME ACTIONS (move, win, chat)
        with self.lock:
            code = info.get("room_code")
            room = self.rooms.get(code)
            if not room or not room.started:
                return
            seat = room.get_seat(handler)
            if seat is None:
                return

        if t == "move":
            with self.lock:
                # Validate ns against the seats the game started with, not the
                # current member count (mid-game leaves shrink members).
                seat_count = (max(room.all_seats) + 1) if room.all_seats else 2
                next_seat = msg.get("ns")
                if not isinstance(next_seat, int) or next_seat < 0 or next_seat >= seat_count:
                    next_seat = (seat + 1) % max(2, seat_count)
                room.current_seat = next_seat
                r_mode = getattr(room, "mode", "Classic")
                is_blitz = (r_mode.lower() == "blitz" or r_mode == "Blitz Mode")
                room.turn_deadline = time.time() + (11.0 if is_blitz else 18.0)
                out = dict(msg)
                out["t"] = "move"
                out["s"] = seat
                out["ns"] = next_seat
                payload = json.dumps(out)
                room.broadcast(payload, self._send_locked)
            return

        if t in ("player_won", "win"):
            with self.lock:
                place = msg.get("v", 1)
                is_over = bool(msg.get("left", False))
                if not hasattr(room, "finished_seats"):
                    room.finished_seats = set()
                room.finished_seats.add(seat)
                out = json.dumps({"t": "player_won", "s": seat, "v": place, "over": is_over})
                room.broadcast(out, self._send_locked)
            return

        if t == "chat":
            text_msg = str(msg.get("m", ""))[:120]
            with self.lock:
                out = json.dumps({"t": "chat", "s": seat, "n": info.get("name"), "m": text_msg})
                room.broadcast(out, self._send_locked)
            return

        if t == "reaction":
            try:
                emoji = int(msg.get("e", -1))
            except (TypeError, ValueError):
                emoji = -1
            if 0 <= emoji < 6:
                with self.lock:
                    out = json.dumps({"t": "reaction", "s": seat, "e": emoji})
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
                self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")

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
        if parsed == "/debug/rooms":
            self.send_debug_rooms()
            return
        super().do_GET()

    def send_debug_rooms(self):
        # Local development helper: only answer on loopback connections.
        if self.client_address[0] not in ("127.0.0.1", "::1", "localhost"):
            self.send_error(http.HTTPStatus.FORBIDDEN)
            return
        with RELAY.lock:
            rooms = {}
            now = time.time()
            for code, room in RELAY.rooms.items():
                left = None
                if room.deadline is not None:
                    left = max(0, int(room.deadline - now))
                rooms[code] = {
                    "players": [room.names.get(s, "") for s in sorted(room.members)],
                    "started": room.started,
                    "left": left,
                }
        payload = json.dumps({"rooms": rooms}).encode("utf-8")
        self.send_response(http.HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        http.server.SimpleHTTPRequestHandler.end_headers(self)
        self.wfile.write(payload)

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
            if not gz.exists() or gz.stat().st_size == 0 or gz.stat().st_mtime < p.stat().st_mtime:
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
    """Support both layouts: project (Build/WebGL) and deploy repo (index.html at root), picking the freshest."""
    here = os.path.dirname(os.path.abspath(__file__))
    parent = os.path.dirname(here)
    candidates = [
        os.path.join(parent, "Build"),
        os.path.join(here, "Build", "WebGL"),
        os.path.join(here, "Build"),
        here,
    ]
    freshest = None
    freshest_mtime = -1
    for c in candidates:
        idx = os.path.join(c, "index.html")
        if os.path.isfile(idx):
            mtime = os.path.getmtime(idx)
            b_dir = os.path.join(c, "Build")
            if os.path.isdir(b_dir):
                for f in os.listdir(b_dir):
                    fp = os.path.join(b_dir, f)
                    if os.path.isfile(fp):
                        mtime = max(mtime, os.path.getmtime(fp))
            if mtime > freshest_mtime:
                freshest_mtime = mtime
                freshest = c
    return freshest or here


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
