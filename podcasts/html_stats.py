import argparse
import yaml
import psycopg2
import time
import os
import pickle
from datetime import datetime
from utils import load_config, connect_to_db

PODCAST_TABLE = 'podcasts'
DEFAULT_WEBPAGE = '/srv/pi.speechcatcher.net/stats.html'
PICKLE_FILE = 'html_stats.pickle'


def parse_args():
    parser = argparse.ArgumentParser(description="Generate Speechcatcher stats HTML")
    parser.add_argument('--webpage', type=str, default=DEFAULT_WEBPAGE, help='Output HTML file path')
    parser.add_argument('--repeat', type=int, default=None, help='Seconds between updates (run continuously)')
    return parser.parse_args()


def connect():
    config = load_config()
    return connect_to_db(database=config["database"],
                         user=config["user"],
                         password=config["password"],
                         host=config["host"],
                         port=config["port"])


def get_hours(cursor, condition):
    query = f"SELECT sum(duration) FROM {PODCAST_TABLE} WHERE {condition};"
    cursor.execute(query)
    result = cursor.fetchone()[0]
    return float(result) / 3600. if result else 0.


def load_previous_stats():
    try:
        with open(PICKLE_FILE, "rb") as f:
            return pickle.load(f)
    except Exception:
        print('Could not load previous stats, starting fresh.')
        return 0., 0., 0., 0.


def save_current_stats(transcribed_hours, untranscribed_hours):
    with open(PICKLE_FILE, "wb") as f:
        pickle.dump([time.time(), transcribed_hours, untranscribed_hours,
                     transcribed_hours + untranscribed_hours], f)


def calculate_speed(prev_time, prev_transcribed_hours, current_transcribed_hours):
    if prev_time == 0.:
        return 0.
    interval_hours = (time.time() - prev_time) / 3600.
    return (current_transcribed_hours - prev_transcribed_hours) / interval_hours


def generate_html(transcribed_hours, untranscribed_hours, inprogress_hours, transcribed_ratio, transcription_speed):
    current_datetime = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    return f'''
<html>
<head><title>Speechcatcher dataset stats</title></head>
<body>
<h1>Speechcatcher dataset stats</h1>
<p>Transcribed {transcribed_hours:.2f} hours from {transcribed_hours + untranscribed_hours:.2f} hours in total ({transcribed_ratio * 100:.2f}%).</p>

<svg width="450" height="20">
  <rect width="400" height="15" style="fill:grey" />
  <rect width="{int(transcribed_ratio * 400)}" height="15" style="fill:green" />
</svg>

<p>Current transcription speed is: {transcription_speed:.2f} hours per hour</p>
<p>Currently in progress: {inprogress_hours:.2f} hours </p>
<p>Info queried at: {current_datetime}</p>
</body>
</html>'''


def main():
    args = parse_args()
    while True:
        conn, cursor = connect()

        transcribed_hours = get_hours(cursor, "transcript_file <> '' AND transcript_file <> 'in_progress'")
        untranscribed_hours = get_hours(cursor, "transcript_file = '' OR transcript_file = 'in_progress'")
        inprogress_hours = get_hours(cursor, "transcript_file = 'in_progress'")

        transcribed_ratio = transcribed_hours / (transcribed_hours + untranscribed_hours) if (transcribed_hours + untranscribed_hours) else 0

        prev_time, prev_transcribed_hours, _, _ = load_previous_stats()
        transcription_speed = calculate_speed(prev_time, prev_transcribed_hours, transcribed_hours)

        html_content = generate_html(transcribed_hours, untranscribed_hours, inprogress_hours, transcribed_ratio, transcription_speed)

        with open(args.webpage, 'w') as out_file:
            out_file.write(html_content)

        save_current_stats(transcribed_hours, untranscribed_hours)

        cursor.close()
        conn.close()

        if args.repeat is None:
            break

        time.sleep(args.repeat)


if __name__ == '__main__':
    main()
