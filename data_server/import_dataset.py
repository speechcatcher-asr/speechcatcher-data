import os
import subprocess
import yaml
import psycopg2
from psycopg2.extras import RealDictCursor
import argparse

# Load configuration
with open('../config.yaml', 'r') as file:
    default_config = yaml.safe_load(file)

def get_file_duration(file_path):
    try:
        result = subprocess.run(
            ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', file_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )
        return float(result.stdout)
    except Exception as e:
        print(f"Error getting duration for {file_path}: {e}")
        return -1

def file_needs_import(cursor, file_path):
    cursor.execute("SELECT 1 FROM podcasts WHERE cache_audio_file = %s", (file_path,))
    return cursor.fetchone() is None

def import_file_to_db(cursor, file_path, download_url, podcast_language, whisper_model):
    filename = os.path.basename(file_path)
    duration = get_file_duration(file_path)
    cache_audio_url = f"{download_url}/{filename}"

    cursor.execute("""
        INSERT INTO podcasts (
            podcast_title, episode_title, published_date, retrieval_time, authors,
            language, description, keywords, episode_url, episode_audio_url,
            cache_audio_url, cache_audio_file, transcript_file, duration, type,
            episode_json, model
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """, (
        'N/A', 'N/A', 'N/A', -1, filename, podcast_language, 'N/A', 'N/A',
        'N/A', 'N/A', cache_audio_url, file_path, 'N/A', duration, 'N/A', '{}', whisper_model
    ))

def main(args):
    db_config = {
        'host': args.host,
        'port': args.port,
        'database': args.database,
        'user': args.user,
        'password': args.password
    }

    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    for filename in os.listdir(args.media_directory):
        file_path = os.path.join(args.media_directory, filename)
        if os.path.isfile(file_path) and file_needs_import(cursor, file_path):
            import_file_to_db(cursor, file_path, args.download_url, args.podcast_language, args.whisper_model)
            print(f"Imported {file_path} into the database.")

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import media files into the database.')
    parser.add_argument('--host', default=default_config['host'], help='Database host')
    parser.add_argument('--port', default=default_config['port'], help='Database port')
    parser.add_argument('--database', default=default_config['database'], help='Database name')
    parser.add_argument('--user', default=default_config['user'], help='Database user')
    parser.add_argument('--password', default=default_config['password'], help='Database password')
    parser.add_argument('--media_directory', default=default_config['audio_dataset_location'], help='Directory containing media files')
    parser.add_argument('--download_url', default=default_config['download_destination_url'], help='URL for downloading media files')
    parser.add_argument('--podcast_language', default=default_config['podcast_language'], help='Language of the podcasts')

    args = parser.parse_args()
    main(args)

