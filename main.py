from socketserver import TCPServer

from RequestHandler import RequestHandler

class Server:
    def __init__(self, host="127.0.0.1", port=9999):
        self._server = TCPServer((host, port), RequestHandler)
        self._server.kv = {}

    def run(self):
        self._server.serve_forever()

server = Server()
server.run()