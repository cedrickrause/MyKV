import socketserver

from Errors import CommandError, Error
from ProtocolHandler import ProtocolHandler


class RequestHandler(socketserver.StreamRequestHandler):

    def setup(self):
        super().setup()
        self._protocolHandler = ProtocolHandler()
        self.data = None
        self._kv = self.server.kv

        self._commands = {
            'GET': self.get,
            'SET': self.set,
            'DELETE': self.delete,
            'FLUSH': self.flush,
            'MGET': self.mget,
            'MSET': self.mset
        }

    def handle(self):
        print(f"{self.client_address[0]} sent something")

        request_data = self._protocolHandler.parse(self.rfile)

        print(f"Request data: {request_data}")

        try:
            response_data = self.process_request(request_data)
        except CommandError as exc:
            response_data = Error(exc.args[0])

        print(f"store: {self._kv}")

        print(f"Response data: {response_data}")

        serialized_response = self._protocolHandler.serialize(response_data)

        self.wfile.write(serialized_response.encode('utf-8'))

    def process_request(self, data):
        if not isinstance(data, list):
            try:
                data = data.split()
            except:
                raise CommandError('Request must be list or simple string.')

        if not data:
            raise CommandError('Missing command')

        command = data[0].upper()
        if command not in self._commands:
            raise CommandError('Unrecognized command: %s' % command)

        return self._commands[command](*data[1:])

    def get(self, key):
        return self._kv.get(key)

    def set(self, key, value):
        self._kv[key] = value
        return 1

    def delete(self, key):
        if key in self._kv:
            del self._kv[key]
            return 1
        return 0

    def flush(self):
        kvlen = len(self._kv)
        self._kv.clear()
        return kvlen

    def mget(self, *keys):
        return [self._kv.get(key) for key in keys]

    def mset(self, *items):
        data = zip(items[::2], items[1::2])
        for key, value in data:
            self._kv[key] = value
        return len(data)
