#!/usr/bin/env python3
"""
Script to detect the language of WebVTT transcript files and update the `language` field in the `podcasts` table.

For each row in `podcasts` where `transcript_file` is not empty and not 'in_progress':
  - Load and parse the VTT file
  - Extract all text (timestamps removed)
  - Detect the predominant language of the text
  - If detected language differs from the current `language` value, update the database

Supports a --simulate flag to preview changes without committing.
"""
import os
import argparse
import yaml
import psycopg2
from psycopg2.extras import RealDictCursor

try:
    import webvtt
except ImportError:
    raise ImportError("Please install webvtt-py: pip install webvtt-py")

try:
    from langdetect import detect
except ImportError:
    raise ImportError("Please install langdetect: pip install langdetect")


def parse_vtt_text(file_path):
    """
    Read a .vtt file and return all caption text concatenated.
    """
    text_parts = []
    for caption in webvtt.read(file_path):
        text_parts.append(caption.text)
    return "\n".join(text_parts)


def load_config(path="../config.yaml"):
    """
    Load YAML config and return as dict
    """
    with open(path, 'r') as f:
        return yaml.safe_load(f)


def main():
    # Load defaults from config.yaml
    cfg = load_config()

    parser = argparse.ArgumentParser(
        description="Detect transcript language and update DB entries based on majority language."
    )
    parser.add_argument('--host',     default=cfg.get('host', 'localhost'), help='Database host')
    parser.add_argument('--port',     default=int(cfg.get('port', 5432)), type=int, help='Database port')
    parser.add_argument('--database', default=cfg.get('database', 'speechcatcher'), help='Database name')
    parser.add_argument('--user',     default=cfg.get('user', ''),         help='Database user')
    parser.add_argument('--password', default=cfg.get('password', ''),     help='Database password')
    parser.add_argument('--simulate', action='store_true',                help='Show changes without updating the database')

    args = parser.parse_args()

    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=args.password
    )
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("""
        SELECT podcast_episode_id, transcript_file, language
        FROM podcasts
        WHERE transcript_file IS NOT NULL
          AND transcript_file != ''
          AND transcript_file != 'in_progress';
    """)
    rows = cursor.fetchall()

    for row in rows:
        episode_id = row['podcast_episode_id']
        transcript_path = row['transcript_file']
        current_lang = row.get('language', '') or ''

        if not os.path.isfile(transcript_path):
            print(f"[WARN] Transcript file not found for ID {episode_id}: {transcript_path}")
            continue

        try:
            text = parse_vtt_text(transcript_path)
        except Exception as e:
            print(f"[ERROR] Failed to parse VTT for ID {episode_id}: {e}")
            continue

        if not text.strip():
            print(f"[INFO] No text content for ID {episode_id}, skipping.")
            continue

        try:
            detected = detect(text)
        except Exception as e:
            print(f"[ERROR] Language detection failed for ID {episode_id}: {e}")
            continue

        if detected != current_lang:
            if args.simulate:
                print(f"[SIM] Would update episode {episode_id}: language '{current_lang}' -> '{detected}'")
            else:
                cursor.execute(
                    "UPDATE podcasts SET language = %s WHERE podcast_episode_id = %s;",
                    (detected, episode_id)
                )
                conn.commit()
                print(f"[OK] Updated episode {episode_id}: language '{current_lang}' -> '{detected}'")
        else:
            print(f"[SKIP] Episode {episode_id}: detected '{detected}', no change needed.")

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()

