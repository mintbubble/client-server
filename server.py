import asyncio
import time


class ClientServerProtocol(asyncio.Protocol):
    values = {}

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        resp = self.process_data(data.decode('utf-8').strip('\r\n'))
        self.transport.write(resp.encode('utf-8'))

    def process_data(self, response):
        chunks = response.split(' ')
        if len(chunks) < 2:
            return 'error\nwrong command\n\n'
        if len(chunks[1]) == 0:
            return 'error\nwrong command\n\n'
        if chunks[0] == 'get':
            if len(chunks) > 2:
                return 'error\nwrong command\n\n'
            else:
                return self.get(chunks[1])
        elif chunks[0] == 'put':
            if len(chunks) > 4:
                return 'error\nwrong command\n\n'
            if len(chunks) > 3:
                try:
                    v1 = float(chunks[2])
                    v2 = int(chunks[3])
                except:
                    return 'error\nwrong command\n\n'
                return self.put(chunks[1], chunks[2], chunks[3])
            else:
                try:
                    v1 = float(chunks[2])
                except:
                    return 'error\nwrong command\n\n'
                return self.put(chunks[1], chunks[2])
        else:
            return 'error\nwrong command\n\n'

    def get(self, key):
        res = 'ok\n'
        if key == '*':
            for key, values in self.values.items():
                for value in values:
                    res = res + str(key) + ' ' + str(value[1]) + ' ' + str(value[0]) + '\n'
        else:
            if key in self.values:
                for value in self.values[key]:
                    res = res + str(key) + ' ' + str(value[1]) + ' ' + str(value[0]) + '\n'

        return res + '\n'

    def put(self, key, value, timestamp=None):
        if key == '*':
            return 'error\nkey cannot contain *\n\n'
        if not key in self.values.keys():
            self.values[key] = []
        if timestamp is None: timestamp = int(time.time())
        if int(timestamp) in [x[0] for x in self.values[key]]:
            self.values[key].pop([x[0] for x in self.values[key]].index(int(timestamp)))
        if not (int(timestamp), float(value)) in self.values[key]:
            self.values[key].append((int(timestamp), float(value)))
            self.values[key].sort(key=lambda x: x[0])

        return 'ok\n\n'


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(ClientServerProtocol, host, port)
    server = loop.run_until_complete(coro)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()

# run_server('127.0.0.1', 8181)
