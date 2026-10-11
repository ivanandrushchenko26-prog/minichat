"""Міні-месенджер на Python (лише стандартна бібліотека).

Запуск:  python server.py   (або python3 server.py)
Потім відкрий http://localhost:3000 у кількох вкладках.
"""
import json
import os
import queue
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", 3000))  # на хостингу порт задає сервіс
MAX_HISTORY = 100

clients = []  # черги вихідних повідомлень для кожного підключеного браузера
history = []
lock = threading.Lock()

PAGE = r"""<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Міні-месенджер</title>
<style>
  * { box-sizing: border-box; }
  body { margin: 0; font-family: system-ui, sans-serif; background: #eef1f5; height: 100vh; display: flex; justify-content: center; }
  #app { width: 100%; max-width: 640px; display: flex; flex-direction: column; background: #fff; }
  header { padding: 12px 16px; background: #2f6fed; color: #fff; display: flex; justify-content: space-between; align-items: center; }
  header h1 { font-size: 18px; margin: 0; }
  #online { font-size: 13px; opacity: .85; }
  #messages { flex: 1; overflow-y: auto; padding: 16px; display: flex; flex-direction: column; gap: 8px; }
  .msg { max-width: 75%; padding: 8px 12px; border-radius: 14px; background: #e9ecf1; align-self: flex-start; word-wrap: break-word; }
  .msg.me { background: #2f6fed; color: #fff; align-self: flex-end; }
  .meta { font-size: 11px; opacity: .7; margin-bottom: 2px; }
  form { display: flex; gap: 8px; padding: 12px; border-top: 1px solid #dde1e7; }
  input { flex: 1; padding: 10px 12px; border: 1px solid #c8ced6; border-radius: 20px; font-size: 15px; outline: none; }
  input:focus { border-color: #2f6fed; }
  button { padding: 0 18px; border: 0; border-radius: 20px; background: #2f6fed; color: #fff; font-size: 15px; cursor: pointer; }
  button:hover { background: #1f5bd4; }
</style>
</head>
<body>
<div id="app">
  <header><h1>💬 Міні-месенджер</h1><span id="online"></span></header>
  <div id="messages"></div>
  <form id="form">
    <input id="text" placeholder="Напиши повідомлення..." autocomplete="off" maxlength="500" autofocus>
    <button type="submit">Надіслати</button>
  </form>
</div>
<script>
  var myName = localStorage.getItem('minichat-name');
  while (!myName || !myName.trim()) { myName = prompt('Твоє ім’я:') || ''; }
  myName = myName.trim().slice(0, 20);
  localStorage.setItem('minichat-name', myName);

  var box = document.getElementById('messages');

  function addMessage(m) {
    var div = document.createElement('div');
    div.className = 'msg' + (m.name === myName ? ' me' : '');
    var meta = document.createElement('div');
    meta.className = 'meta';
    var time = new Date(m.time).toLocaleTimeString('uk-UA', { hour: '2-digit', minute: '2-digit' });
    meta.textContent = m.name + ' · ' + time;
    var body = document.createElement('div');
    body.textContent = m.text; // textContent захищає від XSS
    div.appendChild(meta);
    div.appendChild(body);
    box.appendChild(div);
    box.scrollTop = box.scrollHeight;
  }

  var es = new EventSource('/events');
  es.addEventListener('history', function (e) { box.innerHTML = ''; JSON.parse(e.data).forEach(addMessage); });
  es.addEventListener('message', function (e) { addMessage(JSON.parse(e.data)); });
  es.addEventListener('online', function (e) { document.getElementById('online').textContent = 'онлайн: ' + JSON.parse(e.data); });

  document.getElementById('form').addEventListener('submit', function (e) {
    e.preventDefault();
    var input = document.getElementById('text');
    var text = input.value.trim();
    if (!text) return;
    input.value = '';
    fetch('/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: myName, text: text })
    });
  });
</script>
</body>
</html>"""


def sse(event, data):
    text = f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
    return text.encode("utf-8")


def broadcast(event, data):
    payload = sse(event, data)
    with lock:
        for q in clients:
            q.put(payload)


class QuietServer(ThreadingHTTPServer):
    def handle_error(self, request, client_address):
        # браузери часто відкривають і одразу закривають з'єднання: це не помилка
        if isinstance(sys.exc_info()[1], OSError):
            return
        super().handle_error(request, client_address)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # показуємо, хто підключається
        print(self.client_address[0], fmt % args, flush=True)

    def do_GET(self):
        if self.path == "/":
            body = PAGE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/events":
            self.handle_events()
        else:
            self.send_error(404)

    def handle_events(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        q = queue.Queue()
        try:
            with lock:
                snapshot = list(history)
            self.wfile.write(sse("history", snapshot))
            self.wfile.flush()

            with lock:
                clients.append(q)
                count = len(clients)
            broadcast("online", count)

            while True:
                try:
                    self.wfile.write(q.get(timeout=15))
                except queue.Empty:
                    self.wfile.write(b": ping\n\n")  # підтримує з'єднання живим
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            pass
        finally:
            with lock:
                if q in clients:
                    clients.remove(q)
                count = len(clients)
            broadcast("online", count)

    def do_POST(self):
        if self.path != "/send":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            if length <= 0 or length > 10_000:
                raise ValueError("bad size")
            data = json.loads(self.rfile.read(length).decode("utf-8"))
            name = data["name"].strip()[:20]
            text = data["text"].strip()[:500]
            if not name or not text:
                raise ValueError("empty")
        except (ValueError, KeyError, TypeError, AttributeError):
            self.send_error(400)
            return

        msg = {"name": name, "text": text, "time": int(time.time() * 1000)}
        with lock:
            history.append(msg)
            del history[:-MAX_HISTORY]
        broadcast("message", msg)
        self.send_response(204)
        self.end_headers()


if __name__ == "__main__":
    server = QuietServer(("0.0.0.0", PORT), Handler)
    server.daemon_threads = True
    print(f"Месенджер працює: http://localhost:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nЗупинено")
