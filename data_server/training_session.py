import time
import uuid
from typing import Optional

class TrainingSession:
    """In-memory representation of one client training run."""

    def __init__(self, *,
                 language: str,
                 batch_size: int,
                 p_cursor,
                 order: str = "asc",
                 min_duration: float = 0.0,
                 max_duration: Optional[float] = None,
                 config: dict,
                 podcast_columns: str,
                 podcast_columns_list: list[str],
                 sql_table: str,
                 transcript_file_replace_prefix: str,
                 make_local_url):

        self.session_id: str = uuid.uuid4().hex
        self.language = language
        self.batch_size = batch_size
        self.order = order
        self.min_duration = min_duration
        self.max_duration = max_duration

        # Pull dataset once
        where_clauses = ["transcript_file <> %s", "language = %s", "duration >= %s"]
        params: list = ["", language, min_duration]
        if max_duration is not None:
            where_clauses.append("duration <= %s")
            params.append(max_duration)
        where_sql = " AND ".join(where_clauses)

        p_cursor.execute(
            f"SELECT {podcast_columns} FROM {sql_table} WHERE " + where_sql,
            tuple(params)
        )
        records = p_cursor.fetchall()

        self.dataset: list[dict] = [dict(zip(podcast_columns_list, r)) for r in records]
        self.dataset.sort(key=lambda x: x["duration"], reverse=(order == "desc"))

        for item in self.dataset:
            item["transcript_file_url"] = item["transcript_file"].replace(
                transcript_file_replace_prefix, "https://")
            item["local_cache_audio_url"] = make_local_url(item["cache_audio_url"], config)

        self.num_samples = len(self.dataset)
        self.current_epoch = 0
        self.next_index = 0

        self.batches_served: set[tuple[int, int]] = set()
        self.batches_done: set[tuple[int, int]] = set()
        self.logs: list[dict] = []

    def get_next_batch(self):
        if self.num_samples == 0:
            raise RuntimeError("Dataset is empty – nothing to train on.")

        if self.next_index >= self.num_samples:
            self.current_epoch += 1
            self.next_index = 0

        start_idx = self.next_index
        end_idx = min(self.next_index + self.batch_size, self.num_samples)
        batch = self.dataset[start_idx:end_idx]

        batch_id = start_idx
        self.batches_served.add((self.current_epoch, batch_id))
        self.next_index = end_idx
        return batch_id, self.current_epoch, batch

    def mark_batch_done(self, epoch: int, batch_id: int):
        if (epoch, batch_id) not in self.batches_served:
            raise ValueError("batch_id/epoch unknown for this session or not served yet")
        self.batches_done.add((epoch, batch_id))

    def append_log(self, level: str, message: str):
        self.logs.append({
            "ts": time.time(),
            "level": level,
            "msg": message[:4000],
        })

    def status(self):
        return {
            "session_id": self.session_id,
            "language": self.language,
            "batch_size": self.batch_size,
            "order": self.order,
            "current_epoch": self.current_epoch,
            "num_samples": self.num_samples,
            "num_batches_served": len(self.batches_served),
            "num_batches_done": len(self.batches_done),
            "logs": self.logs[-25:],
        }
