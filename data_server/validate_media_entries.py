import subprocess
import psycopg2
import argparse
import os
from utils import load_config, connect_to_db

def check_media_files(simulate):
    # Load configuration and database connection
    config = load_config()
    p_connection, p_cursor = connect_to_db(database=config["database"], user=config["user"], password=config["password"], host=config["host"], port=config["port"])

    # Query to select podcasts to check for media file existence
    p_cursor.execute("SELECT podcast_episode_id, cache_audio_file FROM podcasts;")
    records = p_cursor.fetchall()

    missing_files = []

    for record in records:
        episode_id, audio_file = record

        print(f'Checking existence for:', episode_id, audio_file)

        if not os.path.exists(audio_file):
            missing_files.append((episode_id, audio_file))
            if simulate:
                print(f"Simulate: Would delete entry for missing file: {audio_file}")
                print(f"DB query (not executed): DELETE FROM podcasts WHERE podcast_episode_id = {episode_id};")
            else:
                p_cursor.execute("DELETE FROM podcasts WHERE podcast_episode_id = %s;", (episode_id,))
                print(f"Deleted entry for missing file: {audio_file}")

    if not simulate:
        # Commit the updates to the database
        p_connection.commit()

    # Log missing files with flush after each write
    with open('missing_files.txt', 'w') as file:
        for episode_id, file_name in missing_files:
            file.write(f"{episode_id};{file_name}\n")
            file.flush()

    # Close database connection
    p_cursor.close()
    p_connection.close()

    print("Media file existence check completed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check media file existence and delete missing entries from the database.")
    parser.add_argument('--simulate', action='store_true', help='Simulate the database queries without actually executing them.')

    args = parser.parse_args()
    check_media_files(args.simulate)
