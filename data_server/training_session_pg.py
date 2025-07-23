import time
import uuid
import json
from typing import Optional

class TrainingSession:
    def __init__(self, session_id: Optional[str] = None, backend: str = "pg", redis_url: Optional[str] = None):
        self.session_id = session_id or uuid.uuid4().hex
        self.backend = backend
        self.redis = None

        if backend == "redis":
            if not redis_url:
                raise ValueError("Redis backend requires redis_url")
            import redis  # only if used
            self.redis = redis.Redis.from_url(redis_url, decode_responses=True)

    def _redis_key(self, field: str) -> str:
        return f"training:{self.session_id}:{field}"

    @classmethod
    def create(cls, *,
               p_cursor,
               p_connection,
               language: str,
               batch_size: int,
               sample_order: str,
               min_duration: float,
               max_duration: Optional[float],
               backend: str = "pg",
               redis_url: Optional[str] = None):
        session = cls(backend=backend, redis_url=redis_url)

        # Store metadata in Postgres always
        p_cursor.execute("""
            INSERT INTO training_sessions (session_id, language, batch_size, sample_order, min_duration, max_duration)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (session.session_id, language, batch_size, sample_order, min_duration, max_duration))
        p_connection.commit()

        # For Redis: initialize fast-changing values
        if backend == "redis":
            session.redis.set(session._redis_key("next_index"), 0)
            session.redis.set(session._redis_key("current_epoch"), 0)
            session.redis.set(session._redis_key("batches_done"), json.dumps([]))
            session.redis.set(session._redis_key("logs"), json.dumps([]))

        return session

    def _load_metadata(self, p_cursor):
        p_cursor.execute("SELECT * FROM training_sessions WHERE session_id = %s", (self.session_id,))
        row = p_cursor.fetchone()
        if not row:
            raise ValueError("Invalid session ID")
        self.meta = dict(zip([desc[0] for desc in p_cursor.description], row))

    def get_next_batch(self, p_cursor, p_connection, podcast_table, podcast_columns, podcast_columns_list, dedup_by_hash: bool = True):
        self._load_metadata(p_cursor)
        lang = self.meta["language"]
        sample_order = self.meta["sample_order"]
        min_dur = self.meta["min_duration"]
        max_dur = self.meta["max_duration"]
        batch_size = self.meta["batch_size"]

        # fetch fast-changing state
        if self.backend == "redis":
            next_index = int(self.redis.get(self._redis_key("next_index")) or 0)
            current_epoch = int(self.redis.get(self._redis_key("current_epoch")) or 0)
        else:
            next_index = self.meta["next_index"]
            current_epoch = self.meta["current_epoch"]

        # Build WHERE clause
        where_clauses = ["p.transcript_file <> %s", "p.transcript_file <> %s", "p.language = %s", "p.duration >= %s"]
        params = ["", "in_progress" , lang, min_dur]
        if max_dur is not None:
            where_clauses.append("p.duration <= %s")
            params.append(max_dur)
        where_sql = " AND ".join(where_clauses)

        # Qualify selected columns
        qualified_columns = ", ".join([f"p.{col.strip()}" for col in podcast_columns.split(",")])
        duration_sort = 'DESC' if sample_order == 'desc' else 'ASC'

        # Pagination
        limit_offset_sql = "LIMIT %s OFFSET %s"
        params += [batch_size, next_index]

        if dedup_by_hash:
            # The following query performs global deduplication of podcast episodes based on file content:
            #
            # - Uses DISTINCT ON (fh.file_hash) to ensure only one episode is selected per unique audio file
            # - Sorts first by p.duration, then by p.podcast_episode_id to ensure deterministic selection
            # - Joins filehashes via file_path = cache_audio_file (both absolute paths, guaranteed unique)
            # - Returns exactly one row per file hash -> ensures no duplicate media in a batch
            # - Applies pagination (LIMIT/OFFSET) only after deduplication
            #
            # This ensures curriculum-style sampling (shortest audio first) and stable batching.
            query = f"""
                SELECT *
                FROM (
                    SELECT DISTINCT ON (fh.file_hash) {qualified_columns}, p.duration AS dur
                    FROM {podcast_table} p
                    JOIN filehashes fh ON fh.file_path = p.cache_audio_file
                    WHERE {where_sql}
                    ORDER BY fh.file_hash, p.duration {duration_sort}, p.podcast_episode_id
                ) AS deduped
                ORDER BY deduped.dur {duration_sort}
                {limit_offset_sql}
            """
        else:
            query = f"""
                SELECT {qualified_columns}
                FROM {podcast_table} p
                WHERE {where_sql}
                ORDER BY p.duration {duration_sort}
                {limit_offset_sql}
            """

        p_cursor.execute(query, tuple(params))
        rows = p_cursor.fetchall()
        batch = [dict(zip(podcast_columns_list, r)) for r in rows]

        if not batch:
            current_epoch += 1
            next_index = 0
            self._update_state(p_cursor, p_connection, current_epoch, next_index)
            raise RuntimeError("End of epoch reached")

        batch_id = next_index
        next_index += len(batch)

        self._update_state(p_cursor, p_connection, current_epoch, next_index)

        return batch_id, current_epoch, batch

    def _update_state(self, p_cursor, p_connection, epoch, next_index):
        if self.backend == "redis":
            self.redis.set(self._redis_key("current_epoch"), epoch)
            self.redis.set(self._redis_key("next_index"), next_index)
        else:
            p_cursor.execute("""
                UPDATE training_sessions
                SET current_epoch = %s, next_index = %s
                WHERE session_id = %s
            """, (epoch, next_index, self.session_id))
            p_connection.commit()

    def mark_batch_done(self, p_cursor, p_connection, epoch, batch_id):
        if self.backend == "redis":
            done = json.loads(self.redis.get(self._redis_key("batches_done")) or "[]")
            if [epoch, batch_id] not in done:
                done.append([epoch, batch_id])
                self.redis.set(self._redis_key("batches_done"), json.dumps(done))
        else:
            self._load_metadata(p_cursor)
            done = self.meta["batches_done"]
            if [epoch, batch_id] not in done:
                done.append([epoch, batch_id])
                p_cursor.execute("""
                    UPDATE training_sessions
                    SET batches_done = %s
                    WHERE session_id = %s
                """, (json.dumps(done), self.session_id))
                p_connection.commit()

    def append_log(self, p_cursor, p_connection, level: str, message: str):
        log_entry = {
            "ts": time.time(),
            "level": level,
            "msg": message[:4000]
        }

        if self.backend == "redis":
            logs = json.loads(self.redis.get(self._redis_key("logs")) or "[]")
            logs.append(log_entry)
            logs = logs[-25:]
            self.redis.set(self._redis_key("logs"), json.dumps(logs))
        else:
            self._load_metadata(p_cursor)
            logs = self.meta["logs"]
            logs.append(log_entry)
            logs = logs[-25:]
            p_cursor.execute("""
                UPDATE training_sessions
                SET logs = %s
                WHERE session_id = %s
            """, (json.dumps(logs), self.session_id))
            p_connection.commit()

    def status(self, p_cursor):
        self._load_metadata(p_cursor)

        if self.backend == "redis":
            current_epoch = int(self.redis.get(self._redis_key("current_epoch")) or 0)
            batches_done = json.loads(self.redis.get(self._redis_key("batches_done")) or "[]")
            logs = json.loads(self.redis.get(self._redis_key("logs")) or "[]")
        else:
            current_epoch = self.meta["current_epoch"]
            batches_done = self.meta["batches_done"]
            logs = self.meta["logs"]

        return {
            "session_id": self.session_id,
            "language": self.meta["language"],
            "batch_size": self.meta["batch_size"],
            "sample_order": self.meta["sample_order"],
            "current_epoch": current_epoch,
            "num_batches_done": len(batches_done),
            "logs": logs[-25:],
        }

    def commit(self, p_cursor, p_connection):
        """Write Redis-stored session state back to PostgreSQL."""
        if self.backend != "redis":
            return  # No-op if not using Redis

        current_epoch = int(self.redis.get(self._redis_key("current_epoch")) or 0)
        next_index = int(self.redis.get(self._redis_key("next_index")) or 0)
        batches_done = json.loads(self.redis.get(self._redis_key("batches_done")) or "[]")
        logs = json.loads(self.redis.get(self._redis_key("logs")) or "[]")

        p_cursor.execute("""
            UPDATE training_sessions
            SET current_epoch = %s,
                next_index = %s,
                batches_done = %s,
                logs = %s
            WHERE session_id = %s
        """, (
            current_epoch,
            next_index,
            json.dumps(batches_done),
            json.dumps(logs),
            self.session_id
        ))
        p_connection.commit()

    def delete(self, p_cursor, p_connection):
        p_cursor.execute("DELETE FROM training_sessions WHERE session_id = %s", (self.session_id,))
        p_connection.commit()

        if self.backend == "redis":
            for key in ["next_index", "current_epoch", "batches_done", "logs"]:
                self.redis.delete(self._redis_key(key))

