import http.server as hs
import threading
import time


class HttpServer:
    def __init__(self):
        self.requests = []
        self.responses = []
        self.headers = []
        self._ready_event = None
        self._stop_event = None
        self._http_server = None
        self._http_server_thread = None

    def _serve(self):
        self._http_server.serve_forever()
        self._stop_event.set()
    
    def __enter__(self):
        headers = self.headers
        requests = self.requests
        responses = self.responses

        class Handler(hs.BaseHTTPRequestHandler):
            def do_GET(self):
                try:
                    headers.append({
                        key: value
                        for key, value in self.headers.items()})
                    try:
                        wait_ms, code, content_type, body = responses.pop(0)
                    except IndexError:
                        wait_ms, code, content_type, body = 0, 200, None, None
                    time.sleep(wait_ms / 1000)
                    self.send_response(code)
                    if content_type:
                        self.send_header('Content-Type', content_type)
                    if body:
                        self.send_header('Content-Length', len(body))
                    self.end_headers()
                    if body:
                        self.wfile.write(body)
                except BrokenPipeError:
                    pass  # Client disconnected early, no biggie.

        self._stop_event = threading.Event()
        self._http_server = hs.HTTPServer(
            ('', 0),
            Handler,
            bind_and_activate=True)
        self._http_server_thread = threading.Thread(target=self._serve)
        self._http_server_thread.start()
        return self
    
    def __exit__(self, _ex_type, _ex_value, _ex_tb):
        self._http_server.shutdown()
        self._http_server.server_close()
        self._stop_event.set()

    @property
    def port(self):
        return self._http_server.server_port
