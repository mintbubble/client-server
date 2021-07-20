import socket
import time


class ClientError(Exception):
    pass


class Client:
    def __init__(self, host, port, timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout

    def send(self, text):
        with socket.create_connection((self.host, self.port), self.timeout) as sock:
            sock.sendall(text.encode("utf8"))
            buf = sock.recv(1024)
            return buf.decode('utf-8')

    def put(self, metric, value, timestamp=None):
        res = self.send(
            'put ' + str(metric) + ' ' + str(value) + ' ' + str(timestamp if timestamp else int(time.time())) + '\n')
        if res[0:3] != 'ok\n':
            raise ClientError(res)

    def get(self, metric):
        res = self.send('get ' + str(metric) + '\n')
        if res[0:3] != 'ok\n':
            raise ClientError(res)
        ret = dict()
        lines = res.split('\n')
        for l in lines[1:-2]:
            v = l.split(' ')
            try:
                res_metric = v[0]
                res_value = float(v[1])
                res_timestamp = int(v[2])
            except:
                raise ClientError(res)

            if not res_metric in ret:
                ret[res_metric] = list()
            ret[res_metric].append((res_timestamp, res_value))
            ret[res_metric].sort(key=lambda x: x[0])

        return ret
