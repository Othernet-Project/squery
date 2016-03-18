import contextlib
import sys

from gevent.queue import Queue
from pyqlizator import Connection


if sys.version_info[0] >= 3:
    integer_types = int,
else:
    import __builtin__
    integer_types = int, __builtin__.long


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
