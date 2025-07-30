import argparse
import subprocess
import os
import psycopg2
import shutil
import json
from urllib.parse import urljoin

def load_schema(cursor):
    schema_file = "schema.psql"
    with open(schema_file, "r") as f:
        schema = f.read()
    cursor.execute(schema)
    print("Schema loaded successfully.")

def get_free_space(path):
    """Return the free space in GB for the given path."""
    disk_usage = shutil.disk_usage(path)
    return disk_usage.free / (2**30)  # Convert bytes to GB

def download_file(url, destination):
    print(f"About to download from URL: {url}")
    print(f"Attempting to download file to {destination}")

    # Ensure the directory exists
    os.makedirs(os.path.dirname(destination), exist_ok=True)

    # Use aria2c to download the file with up to 5 retries
    result = subprocess.run(['aria2c', '--max-tries=5', '-x', '16', '-s', '16', url, '-o', destination], capture_output=True, text=True)

    if result.returncode == 0:
        print(f"Successfully downloaded and stored file at {destination}")
        return True
    else:
        print(f"Failed to download file from {url}")
        print("Error:", result.stderr)
        return False

def main():
    # Load config.yaml
    with open(os.path.join(os.path.dirname(__file__), '../config.yaml'), 'r') as config_file:
        config = yaml.safe_load(config_file)

    # Set defaults from YAML
    default_api_url = config.get("server_api_url", "").rstrip('/')
    default_api_key = config.get("secret_api_key", "")
    default_db_name = config.get("database", "speechcatcher")
    default_db_user = config.get("user", "speechcatcher")
    default_db_password = config.get("password", "")
    default_db_host = config.get("host", "localhost")
    default_db_port = config.get("port", "5432")

    # Argument parser setup
    parser = argparse.ArgumentParser(description="Clone podcast entries from a remote server to a local database.")
    parser.add_argument("--remote-api-url", default=default_api_url, help="URL of the remote server API")
    parser.add_argument("--local-cache-destinations", default="", help="Comma-separated local cache destination paths")
    parser.add_argument("--http-base-paths", default="", help="Comma-separated HTTP base paths corresponding to local destinations")
    parser.add_argument("--api-access-key", default=default_api_key, help="API access key for the remote server")
    parser.add_argument("--db-name", default=default_db_name, help="Database name")
    parser.add_argument("--db-user", default=default_db_user, help="Database user")
    parser.add_argument("--db-password", default=default_db_password, help="Database password")
    parser.add_argument("--db-host", default=default_db_host, help="Database host")
    parser.add_argument("--db-port", default=default_db_port, help="Database port")
    parser.add_argument("--simulate", action="store_true", help="Simulate the process without committing to the database")
    parser.add_argument("--include-files-without-transcripts", action="store_true", help="Include files even if transcripts are missing")

    args = parser.parse_args()

    # Unpack arguments
    remote_api_url = args.remote_api_url
    local_cache_destinations = args.local_cache_destinations.split(',')
    http_base_paths = args.http_base_paths.split(',')
    api_access_key = args.api_access_key
    simulate = args.simulate
    include_files_without_transcripts = args.include_files_without_transcripts

    # Validate path count
    assert len(local_cache_destinations) == len(http_base_paths), "Number of local cache destinations must match number of HTTP base paths."

    # Connect to the local database using the arguments
    conn = psycopg2.connect(
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
        host=args.db_host,
        port=args.db_port,
    )
    cursor = conn.cursor()
    # Load the schema if the table doesn't exist
    cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'podcasts')")
    if not cursor.fetchone()[0]:
        load_schema(cursor)
        if not simulate:
            conn.commit()
            print("Committed schema changes to the database.")
        else:
            print("Simulation: Would commit schema changes to the database.")

    # Fetch entries from the remote server
    print(f"Fetching entries from {remote_api_url}")
    response = subprocess.run(['curl', f"{remote_api_url}/apiv1/get_every_episode_list/{api_access_key}"], capture_output=True, text=True)

    if response.returncode != 0:
        print("Failed to fetch entries from the remote server.")
        return

    entries = json.loads(response.stdout)
    print(f"Fetched {len(entries)} entries from the remote server.")

    for entry in entries:
        cache_audio_file = entry['cache_audio_file']
        transcript_file = entry['transcript_file']

        if not arg_include_files_without_transcripts:
            if not transcript_file or transcript_file == '' or transcript_file == 'in_progress':
                print('Not cloning media file without transcript:', cache_audio_file)
                continue

        # Determine the destination path with sufficient free space
        dest_path = None
        for destination in local_cache_destinations:
            if get_free_space(destination) > 4:  # Check if there's more than 4GB free
                dest_path = destination
                break

        if not dest_path:
            print("No destination with sufficient free space available. Aborting.")
            break

        local_audio_path = os.path.join(dest_path, os.path.basename(cache_audio_file))
        local_vtt_path = os.path.join(dest_path, 'vtts', os.path.splitext(os.path.basename(cache_audio_file))[0] + '.vtt')

        print(f"Destination for audio file: {local_audio_path}")
        print(f"Destination for VTT file: {local_vtt_path}")

        # Download the cache media file and VTT file
        cache_audio_url = entry['cache_audio_url']
        vtt_url = urljoin(cache_audio_url.rsplit('/', 1)[0] + '/', 'vtts/' + os.path.splitext(os.path.basename(cache_audio_url))[0] + '.vtt')

        if download_file(cache_audio_url, local_audio_path) and download_file(vtt_url, local_vtt_path):
            # Update the entry's file paths
            entry['cache_audio_file'] = local_audio_path
            entry['transcript_file'] = local_vtt_path

            # Insert the updated entry into the local database
            sql = '''
                INSERT INTO podcasts (
                    podcast_title, episode_title, published_date, retrieval_time, authors, language,
                    description, keywords, episode_url, episode_audio_url, cache_audio_url,
                    cache_audio_file, transcript_file, duration, type, episode_json, model
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            data = (
                entry['podcast_title'], entry['episode_title'], entry['published_date'], entry['retrieval_time'],
                entry['authors'], entry['language'], entry['description'], entry['keywords'], entry['episode_url'],
                entry['episode_audio_url'], entry['cache_audio_url'], entry['cache_audio_file'], entry['transcript_file'],
                entry['duration'], entry['type'], entry['episode_json'], entry['model']
            )

            print(f"Executing SQL: {cursor.mogrify(sql, data).decode('utf-8')}")

            if not simulate:
                cursor.execute(sql, data)
                conn.commit()
                print("Committed entry to the database.")
            else:
                print("Simulation: Would commit entry to the database.")
        else:
            print("Failed to download files for entry. Skipping.")

    cursor.close()
    conn.close()
    print("Database connection closed.")

if __name__ == "__main__":
    main()

