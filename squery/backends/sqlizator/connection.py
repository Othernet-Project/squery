import contextlib
import sys

import msgpack

from gevent import socket
from gevent.queue import Queue


if sys.version_info[0] >= 3:
    integer_types = int,
else:
    import __builtin__
    integer_types = int, __builtin__.long


class Socket(object):

    def __init__(self, host, port, timeout=2):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.connect((host, port))
        self._sock.settimeout(timeout)

    def close(self):
        self._sock.shutdown(socket.SHUT_RDWR)
        self._sock.close()

    def recv(self, buf_size=4096):
        while True:
            buf = self._sock.recv(buf_size)
            if not buf:
                return
            # yield received chunks immediately instead of collecting
            # all together so that streaming parsers can be utilized
            yield buf
            # in case the received package was smaller than `buf_size`
            # prevent the generator from doing any more work
            if len(buf) < buf_size:
                return

    def send(self, data):
        self._sock.sendall(data)


class OperationalError(Exception):

    def __init__(self, code, message):
        self.code = code
        self.message = message
        msg = '[{}] {}'.format(code, message)
        super(OperationalError, self).__init__(msg)


class Connection(object):
    SQLITE_DATE_TYPES = ('date', 'datetime', 'timestamp')
    MAX_VARIABLE_NUMBER = 999
    # server commands
    EXECUTE = 1
    EXECUTE_AND_FETCH = 2
    # server reply status codes
    OK = 0
    UNKNOWN_ERROR = 1
    DESERIALIZATION_ERROR = 2
    BAD_MESSAGE = 3
    DATABASE_NOT_FOUND = 4
    INVALID_QUERY = 5
    # in case an error message is not found in the reply
    DEFAULT_MESSAGE = 'Unknown error.'

    socket_cls = Socket
    OperationalError = OperationalError

    _to_primitive_converters = {}
    _from_primitive_converters = {}

    def __init__(self, host, port, database):
        self._dbname = database
        try:
            self._socket = self.socket_cls(host, port)
        except (socket.error, socket.timeout):
            self._socket = None
            raise

    @classmethod
    def to_primitive(cls, obj):
        try:
            fn = cls._to_primitive_converters[type(obj)]
        except KeyError:
            return obj
        else:
            return fn(obj)

    @classmethod
    def from_primitive(cls, value, type_name):
        try:
            fn = cls._from_primitive_converters[type_name]
        except KeyError:
            return value
        else:
            return fn(value)

    @classmethod
    def register_to_primitive(cls, type_object, fn):
        cls._to_primitive_converters[type_object] = fn

    @classmethod
    def register_from_primitive(cls, type_name, fn):
        cls._from_primitive_converters[type_name] = fn

    def _send(self, data):
        serialized = msgpack.packb(data, default=self.to_primitive)
        try:
            self._socket.send(serialized)
        except (socket.error, socket.timeout):
            self._socket = None
            raise

    def _recv(self):
        unpacker = msgpack.Unpacker()
        self._meta_info = None
        try:
            for data in self._socket.recv():
                unpacker.feed(data)
                for obj in unpacker:
                    reply = self._process_reply(obj)
                    if reply:
                        yield reply
        except (socket.error, socket.timeout):
            self._socket = None
            raise

    def _construct_row(self, data):
        row = dict()
        for ((col_name, col_type), value) in zip(self._meta_info, data):
            row[col_name] = self.from_primitive(value, col_type)
        return row

    def _check_status(self, data):
        # a dict containing ``status`` key holds the information about
        # whether the query was successful or not
        status = data.get('status', self.UNKNOWN_ERROR)
        if status != self.OK:
            message = data.get('message', self.DEFAULT_MESSAGE)
            raise self.OperationalError(status, message)

        return None

    def _process_reply(self, data):
        if isinstance(data, dict):
            return self._check_status(data)

        if data and isinstance(data[-1], (list, tuple)):
            self._meta_info = data
            return None

        return self._construct_row(data)

    def _command(self, operation, sql, *parameters):
        try:
            (params,) = parameters
        except ValueError:
            params = ()

        data = {'operation': operation,
                'database': self._dbname,
                'query': sql,
                'parameters': params}
        self._send(data)
        return self._recv()

    def execute(self, sql, *parameters):
        return self._command(self.EXECUTE,
                             sql,
                             *parameters)

    def fetch(self, sql, *parameters):
        return self._command(self.EXECUTE_AND_FETCH,
                             sql,
                             *parameters)

    @property
    def closed(self):
        return self._socket is None

    def close(self):
        self._socket.close()
        self._socket = None


class ConnectionPool(object):
    connection_cls = Connection

    def __init__(self, maxsize=100, **kwargs):
        if not isinstance(maxsize, integer_types):
            raise TypeError('Expected integer, got %r' % (maxsize, ))
        self._maxsize = maxsize
        self._pool = Queue()
        self._size = 0
        self._conn_params = kwargs

    def get(self):
        if self._size >= self._maxsize or self._pool.qsize():
            return self._pool.get()
        else:
            self._size += 1
            try:
                return self._create_connection()
            except:
                self._size -= 1
                raise

    def put(self, item):
        self._pool.put(item)

    def closeall(self):
        while not self._pool.empty():
            conn = self._pool.get_nowait()
            try:
                conn.close()
            except Exception:
                pass

    def _create_connection(self):
        return self.connection_cls(**self._conn_params)

    @contextlib.contextmanager
    def connection(self):
        conn = self.get()
        try:
            yield conn
        except:
            if conn.closed:
                conn = None
                self.closeall()
            raise
        finally:
            if conn is not None and not conn.closed:
                self.put(conn)

    def execute(self, sql, *parameters):
        with self.connection() as conn:
            return list(conn.execute(sql, *parameters))

    def fetch(self, sql, *parameters):
        with self.connection() as conn:
            return conn.fetch(sql, *parameters)

