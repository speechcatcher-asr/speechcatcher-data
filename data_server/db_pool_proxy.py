import threading
from contextlib import contextmanager
from psycopg2.pool import ThreadedConnectionPool
import psycopg2

class _ThreadLocalState(threading.local):
    def __init__(self):
        self.conn = None
        self.cur = None
        self.in_txn = False  # set when using transaction()

class PooledConnectionProxy:
    """
    Mimics a psycopg2 connection enough for your code:
    - .cursor() returns a per-thread cursor proxy
    - .close() and pool cleanup handled at app teardown
    - .transaction() context manager for multi-statement writes
    """
    def __init__(self, **kwargs):
        minconn = int(kwargs.pop("minconn", 1))
        maxconn = int(kwargs.pop("maxconn", 10))
        self.pool = ThreadedConnectionPool(minconn=minconn, maxconn=maxconn, **kwargs)
        self._tls = _ThreadLocalState()

    def _ensure_conn_cur(self, readonly=True):
        # If we're inside an explicit transaction, reuse the txn connection.
        if self._tls.in_txn:
            if self._tls.cur is None:
                self._tls.cur = self._tls.conn.cursor()
            return self._tls.conn, self._tls.cur

        # Otherwise: get/refresh a thread-local autocommit connection.
        if self._tls.conn is None or self._tls.conn.closed:
            conn = self.pool.getconn()
            conn.autocommit = True  # read-only, single-statement mode
            self._tls.conn = conn
            self._tls.cur = None

        if self._tls.cur is None or self._tls.cur.closed:
            self._tls.cur = self._tls.conn.cursor()

        # When readonly=False without transaction(), we still keep autocommit=True.
        # Use transaction() for atomic multi-statement writes.
        return self._tls.conn, self._tls.cur

    def cursor(self, readonly=True):
        # Return a thin proxy that delegates to the thread-local real cursor.
        pc = self
        class CursorProxy:
            def __getattr__(self, name):
                _, cur = pc._ensure_conn_cur(readonly=readonly)
                return getattr(cur, name)

            def close(self):
                # Only close the cursor (NOT the connection) in readonly mode.
                # For txn mode, the transaction() context manages lifecycle.
                if not pc._tls.in_txn and pc._tls.cur and not pc._tls.cur.closed:
                    try:
                        pc._tls.cur.close()
                    finally:
                        pc._tls.cur = None

        return CursorProxy()

    @contextmanager
    def transaction(self):
        """
        Use for multi-statement writes that must be atomic.
        Example:
            with p_connection.transaction() as cur:
                cur.execute("UPDATE ...")
                cur.execute("INSERT ...")
        """
        if self._tls.in_txn:
            # Nested transactions not supported in this simple wrapper
            raise RuntimeError("Nested transactions are not supported")

        conn = self.pool.getconn()
        try:
            conn.autocommit = False
            cur = conn.cursor()
            # mark txn state
            self._tls.in_txn = True
            self._tls.conn = conn
            self._tls.cur = cur
            try:
                yield cur
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                try:
                    cur.close()
                finally:
                    self._tls.cur = None
                    self._tls.conn = None
                    self._tls.in_txn = False
                    self.pool.putconn(conn)
        except:
            # Ensure pool gets a good connection back even on unexpected errors
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    self.pool.putconn(conn)
                except Exception:
                    pass
            raise

    def closeall(self):
        self.pool.closeall()

