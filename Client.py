import io
import socket

from Errors import CommandError, Error
from ProtocolHandler import ProtocolHandler

class Client(object):
    def __init__(self, host='localhost', port=9999):
        self._protocol = ProtocolHandler()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((host, port))

    def execute(self, *args):
        request = self._protocol.serialize(args)
        self._socket.sendall(request.encode())
        self._socket.sendall(b"\n")
        response = self._protocol.parse(io.BytesIO(self._socket.recv(1024)))
        # TODO After the first call there is a broken pipe error here.. dunno why
        print("Sent:    ", args)
        print("Received:", response)
        if isinstance(response, Error):
            raise CommandError(response.message)
        return response

    def get(self, key):
        return self.execute('GET', key)

    def set(self, key, value):
        return self.execute('SET', key, value)

    def delete(self, key):
        return self.execute('DELETE', key)

    def flush(self):
        return self.execute('FLUSH')

    def mget(self, *keys):
        return self.execute('MGET', *keys)

    def mset(self, *items):
        return self.execute('MSET', *items)