#!/usr/bin/env python
from http.server import BaseHTTPRequestHandler, HTTPServer
import json


class Server(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

    def do_HEAD(self):
        self._set_headers()

    def do_GET(self):
        request_path = self.path
        file_to_retrieve = request_path.split("/")[-1]
        with open(f"./newest_data/{file_to_retrieve}.json", "rb") as f:
            self._set_headers()
            self.wfile.write(f.read())


def run(server_class=HTTPServer, handler_class=Server, port=8008):
    server_address = ("", port)
    httpd = server_class(server_address, handler_class)
    print(f"Starting httpd on port {port}...")
    httpd.serve_forever()


if __name__ == "__main__":
    run()
