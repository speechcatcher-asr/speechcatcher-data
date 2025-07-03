#!/usr/bin/env python3

import os
import hashlib
import argparse
from tqdm import tqdm
import psycopg2
from utils import load_config, connect_to_db


def compute_sha256(filepath):
    """Compute SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()
    except FileNotFoundError:
        return None


def ensure_filehashes_table(cursor, conn):
    """Create filehashes table and indexes if they do not exist."""
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS filehashes (
        filehash_id SERIAL PRIMARY KEY,
        podcast_episode_id INTEGER REFERENCES podcasts(podcast_episode_id),
        file_path TEXT UNIQUE,
        file_hash TEXT,
        file_type VARCHAR(32),
        last_verified TIMESTAMP DEFAULT NOW()
    );
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_filehashes_file_hash ON filehashes (file_hash);")
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_filehashes_file_path ON filehashes (file_path);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_filehashes_episode_id ON filehashes (podcast_episode_id);")

    conn.commit()  # ✅ Persist schema changes


def fetch_podcast_files(cursor):
    """Fetch all podcast file paths (audio + transcript)."""
    cursor.execute("SELECT podcast_episode_id, cache_audio_file, transcript_file FROM podcasts;")
    return cursor.fetchall()


def fetch_existing_hashes(cursor):
    """Get dict of existing file paths and their stored hashes."""
    cursor.execute("SELECT file_path, file_hash FROM filehashes;")
    return dict(cursor.fetchall())


def insert_filehash(cursor, podcast_episode_id, filepath, filehash, filetype):
    """Insert file hash row into DB (deferred commit)."""
    cursor.execute("""
    INSERT INTO filehashes (podcast_episode_id, file_path, file_hash, file_type)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (file_path) DO NOTHING;
    """, (podcast_episode_id, filepath, filehash, filetype))


def check_integrity(existing_hashes):
    """Check current hash of file against stored hash."""
    corrupted = []
    tqdm.write("🔍 Running integrity check on existing hashed files...")
    for filepath, stored_hash in tqdm(existing_hashes.items(), desc="Checking integrity", unit="file"):
        current_hash = compute_sha256(filepath)
        if current_hash is None:
            corrupted.append((filepath, "File not found"))
        elif current_hash != stored_hash:
            corrupted.append((filepath, "Hash mismatch"))

    if not corrupted:
        tqdm.write("✅ All files verified OK.")
    else:
        tqdm.write("❌ Some files failed integrity check:")
        for f, reason in corrupted:
            tqdm.write(f"  - {f}: {reason}")


def report_duplicates(cursor):
    """Find and report files with the same hash (potential duplicates)."""
    tqdm.write("🔁 Looking for duplicate files...")
    cursor.execute("""
    SELECT file_hash, array_agg(file_path), COUNT(*)
    FROM filehashes
    GROUP BY file_hash
    HAVING COUNT(*) > 1;
    """)
    rows = cursor.fetchall()
    if not rows:
        tqdm.write("✅ No duplicate files found based on SHA256 hashes.")
        return

    tqdm.write("\n❗ Duplicate files detected (same SHA256):\n")
    for file_hash, file_paths, count in rows:
        tqdm.write(f"Hash: {file_hash} ({count} files)")
        for path in file_paths:
            tqdm.write(f"  - {path}")
        tqdm.write("")


def main():
    parser = argparse.ArgumentParser(description="Store and verify file hashes for podcast files.")
    parser.add_argument("--check", action="store_true", help="Run integrity check (verify files vs. stored hashes).")
    parser.add_argument("--report-duplicates", action="store_true", help="Report files with duplicate hashes.")
    parser.add_argument("--batch-size", type=int, default=20, help="Number of inserts between DB commits (default: 20).")
    args = parser.parse_args()

    config = load_config()
    conn, cursor = connect_to_db(config["database"], config["user"], config["password"],
                                 host=config["host"], port=config["port"])

    ensure_filehashes_table(cursor, conn)

    if args.check:
        tqdm.write("Mode: Integrity check")
        existing_hashes = fetch_existing_hashes(cursor)
        check_integrity(existing_hashes)
        conn.close()
        return

    if args.report_duplicates:
        tqdm.write("Mode: Report duplicates")
        report_duplicates(cursor)
        conn.close()
        return

    # Default: compute and insert new hashes
    tqdm.write("Mode: Add missing file hashes to database")
    tqdm.write(f"Batch size: {args.batch_size}")

    file_records = fetch_podcast_files(cursor)
    existing_hashes = fetch_existing_hashes(cursor)

    inserted = 0
    total_inserted = 0

    for podcast_episode_id, audio_path, transcript_path in tqdm(file_records, desc="Processing files", unit="episode"):
        for path, ftype in [(audio_path, 'audio'), (transcript_path, 'transcript')]:
            if not path or path in existing_hashes:
                continue
            hashval = compute_sha256(path)
            if hashval:
                insert_filehash(cursor, podcast_episode_id, path, hashval, ftype)
                existing_hashes[path] = hashval  # prevent duplicate hashing in same run
                inserted += 1
                total_inserted += 1

                if inserted >= args.batch_size:
                    conn.commit()
                    inserted = 0

    if inserted > 0:
        conn.commit()

    conn.close()
    tqdm.write(f"✅ Stored {total_inserted} new file hashes.")


if __name__ == "__main__":
    main()

