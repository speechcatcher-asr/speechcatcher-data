import argparse
import yaml
import psycopg2
import time
import os
import pickle
from datetime import datetime
import matplotlib.pyplot as plt
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

def get_file_count(cursor, condition):
    query = f"SELECT COUNT(*) FROM {PODCAST_TABLE} WHERE {condition};"
    cursor.execute(query)
    return cursor.fetchone()[0]

def get_total_size(cursor, condition):
    # Query to select the cache paths of the files
    query = f"SELECT cache_audio_file FROM {PODCAST_TABLE} WHERE {condition};"
    cursor.execute(query)
    files = cursor.fetchall()

    # Calculate the total size of the files
    total_size = 0
    for file in files:
        file_path = file[0]
        if os.path.exists(file_path):
            total_size += os.path.getsize(file_path)
        else:
            print(f"Warning: File not found - {file_path}")

    return total_size

def get_distinct_authors(cursor, condition):
    query = f"SELECT COUNT(DISTINCT authors) FROM {PODCAST_TABLE} WHERE {condition};"
    cursor.execute(query)
    return cursor.fetchone()[0]

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

def generate_duration_histogram(cursor, condition, output_file):
    query = f"SELECT duration FROM {PODCAST_TABLE} WHERE {condition};"
    cursor.execute(query)
    durations = [float(row[0]) / 60. for row in cursor.fetchall()]

    plt.figure(figsize=(10, 5))
    plt.hist(durations, bins=300, color='blue', alpha=0.7)
    plt.title('Distribution of Durations')
    plt.xlabel('Duration (minutes)')
    plt.ylabel('Frequency')
    plt.savefig(output_file, format='svg')
    plt.close()

def generate_html(transcribed_hours, untranscribed_hours, inprogress_hours, transcribed_ratio, transcription_speed,
                  total_files, total_size, distinct_authors, transcribed_files, transcribed_size, transcribed_authors):
    current_datetime = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
    return f'''
<html>
<head>
    <title>Speechcatcher Dataset Stats</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            line-height: 1.6;
        }}
        h1 {{
            color: #333;
        }}
        p {{
            color: #555;
            margin: 0.1em 0;
            padding: 0;
        }}
        .stats {{
            margin-bottom: 20px;
        }}
        .progress-bar {{
            width: 400px;
            height: 20px;
            background-color: #ddd;
            border-radius: 5px;
            overflow: hidden;
            margin-top: 10px;
        }}
        .progress {{
            height: 100%;
            background-color: #4caf50;
            text-align: center;
            line-height: 20px;
            color: white;
        }}
        img {{
            max-width: 100%;
            height: auto;
            margin-top: 20px;
        }}
    </style>
</head>
<body>
    <h1>Speechcatcher Dataset Stats</h1>
    <div class="stats">
        <p>Transcribed <strong>{transcribed_hours:.2f}</strong> hours from <strong>{transcribed_hours + untranscribed_hours:.2f}</strong> hours in total (<strong>{transcribed_ratio * 100:.2f}%</strong>).</p>
        <div class="progress-bar">
            <div class="progress" style="width: {int(transcribed_ratio * 100)}%">{int(transcribed_ratio * 100)}%</div>
        </div>
        <br/>
        <p>Current transcription speed: <strong>{transcription_speed:.2f}</strong> hours per hour</p>
        <p>Currently in progress: <strong>{inprogress_hours:.2f}</strong> hours</p>
        <p>Total files: <strong>{total_files}</strong></p>
        <p>Total size: <strong>{total_size/(1024.*1024.*1024.):.2f}</strong> GB</p>
        <p>Distinct authors: <strong>{distinct_authors}</strong></p>
        <p>Transcribed files: <strong>{transcribed_files}</strong></p>
        <p>Transcribed size: <strong>{transcribed_size/(1024.*1024.*1024.):.2f}</strong> GB</p>
        <p>Transcribed authors: <strong>{transcribed_authors}</strong></p>
        <p>Info queried at: <strong>{current_datetime}</strong></p>
    </div>
    <img src="duration_histogram.svg" alt="Duration Histogram">
</body>
</html>
'''

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

        total_files = get_file_count(cursor, "1=1")
        total_size = get_total_size(cursor, "1=1")
        distinct_authors = get_distinct_authors(cursor, "1=1")

        transcribed_files = get_file_count(cursor, "transcript_file <> '' AND transcript_file <> 'in_progress'")
        transcribed_size = get_total_size(cursor, "transcript_file <> '' AND transcript_file <> 'in_progress'")
        transcribed_authors = get_distinct_authors(cursor, "transcript_file <> '' AND transcript_file <> 'in_progress'")

        html_content = generate_html(transcribed_hours, untranscribed_hours, inprogress_hours, transcribed_ratio, transcription_speed,
                                     total_files, total_size, distinct_authors, transcribed_files, transcribed_size, transcribed_authors)

        print('Time:', time.time())

        print('Writing HTML content to:', args.webpage)
        with open(args.webpage, 'w') as out_file:
            out_file.write(html_content)

        save_current_stats(transcribed_hours, untranscribed_hours)

        histogram_outfile = '/'.join(args.webpage.split('/')[:-1]) + '/duration_histogram.svg'

        print('Writing duration_histogram.svg to:', histogram_outfile)
        generate_duration_histogram(cursor, "1=1", histogram_outfile)

        cursor.close()
        conn.close()

        if args.repeat is None:
            break

        time.sleep(args.repeat)

if __name__ == '__main__':
    main()

