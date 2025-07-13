from typing import BinaryIO

from Errors import Disconnect, CommandError, Error


class ProtocolHandler:

    def __init__(self):
        self.handlers = {
            b'+': self.handle_simple_string,
            b'-': self.handle_error,
            b':': self.handle_integer,
            b'$': self.handle_string,
            b'*': self.handle_array,
            b'%': self.handle_dict,
        }


    def parse(self, rfile: BinaryIO):
        first_byte = rfile.read(1)
        if not first_byte:
            raise Disconnect()

        try:
            return self.handlers[first_byte](rfile)
        except KeyError:
            raise CommandError('Bad Request')


    def handle_simple_string(self, rfile: BinaryIO):
        return rfile.readline().rstrip()


    def handle_error(self, rfile: BinaryIO):
        return Error(rfile.readline().rstrip())


    def handle_integer(self, rfile: BinaryIO):
        return int(rfile.readline().rstrip())


    def handle_string(self, rfile: BinaryIO):
        length = int(rfile.readline().rstrip())
        if length == -1:
            return None
        length += 2
        return rfile.readline().rstrip()


    def handle_array(self, rfile: BinaryIO):
        num_elements = int(rfile.readline().rstrip())
        return [self.parse(rfile) for _ in range(num_elements)]


    def handle_dict(self, rfile: BinaryIO):
        num_items = int(rfile.readline().rstrip())
        elements = [self.parse(rfile) for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))


    def serialize(self, data):
        if isinstance(data, bytes):
            data = data.decode('utf-8')

        if isinstance(data, str):
            return f"${len(data)}\r\n{data}\r\n"
        elif isinstance(data, int):
            return f":{data}\r\n"
        elif isinstance(data, Error):
            return f"-{data.message}\r\n"
        elif isinstance(data, (list, tuple)):
            head = f"*{len(data)}\r\n"
            body = ''.join([self.serialize(sub_data) for sub_data in data])
            return ''.join([head, body])
        elif isinstance(data, dict):
            head = f"%{len(data)}\r\n"
            body = ''
            for key in data:
                body += self.serialize(key)
                body += self.serialize(data[key])
            return ''.join([head, body])
        elif data is None:
            return f"$-1\r\n"
        else:
            raise CommandError('unrecognized type: %s' % type(data))


