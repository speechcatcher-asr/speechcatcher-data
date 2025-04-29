import os
import subprocess
import yaml
import psycopg2
from psycopg2.extras import RealDictCursor

# Load configuration
with open('../config.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Database connection details
db_config = {
    'host': config['host'],
    'port': config['port'],
    'database': config['database'],
    'user': config['user'],
    'password': config['password']
}

# Directory containing media files
media_directory = config['audio_dataset_location']
download_url = config['download_destination_url']

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

def import_file_to_db(cursor, file_path):
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
        'N/A', 'N/A', 'N/A', -1, filename, config['podcast_language'], 'N/A', 'N/A',
        'N/A', 'N/A', cache_audio_url, file_path, 'N/A', duration, 'N/A', '{}', config['whisper_model']
    ))

def main():
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    for filename in os.listdir(media_directory):
        file_path = os.path.join(media_directory, filename)
        if os.path.isfile(file_path) and file_needs_import(cursor, file_path):
            import_file_to_db(cursor, file_path)
            print(f"Imported {file_path} into the database.")

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()

