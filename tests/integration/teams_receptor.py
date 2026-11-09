"""Receptor HTTP local de integración; no conecta con Teams ni imprime payloads."""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer

mensajes = []


class Receptor(BaseHTTPRequestHandler):
    def do_POST(self):
        datos = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if set(datos) != {"text"} or not isinstance(datos["text"], str):
            self.send_error(400)
            return
        mensajes.append(datos)
        self.send_response(202)
        self.end_headers()

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(mensajes).encode())

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 8765), Receptor).serve_forever()
