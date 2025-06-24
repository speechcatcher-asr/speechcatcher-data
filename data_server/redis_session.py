import pickle
import redis
from typing import Optional, Iterator

class RedisSessionDict:
    def __init__(self, redis_url: str, prefix: str = "training_session:", ttl: Optional[int] = 3600):
        self.redis = redis.Redis.from_url(redis_url)
        self.prefix = prefix
        self.ttl = ttl

    def _key(self, session_id: str) -> str:
        return f"{self.prefix}{session_id}"

    def __getitem__(self, session_id: str):
        data = self.redis.get(self._key(session_id))
        if data is None:
            raise KeyError(session_id)
        return pickle.loads(data)

    def __setitem__(self, session_id: str, value):
        self.redis.setex(self._key(session_id), self.ttl, pickle.dumps(value))

    def __delitem__(self, session_id: str):
        self.redis.delete(self._key(session_id))

    def __contains__(self, session_id: str) -> bool:
        return self.redis.exists(self._key(session_id)) == 1

    def get(self, session_id: str, default=None):
        try:
            return self[session_id]
        except KeyError:
            return default

    def pop(self, session_id: str, default=None):
        try:
            value = self[session_id]
            del self[session_id]
            return value
        except KeyError:
            return default

    def keys(self) -> Iterator[str]:
        for key in self.redis.scan_iter(match=f"{self.prefix}*"):
            yield key.decode().removeprefix(self.prefix)

    def __repr__(self):
        return f"<RedisSessionDict keys={list(self.keys())}>"

