import subprocess
import psycopg2
from utils import load_config, connect_to_db

def update_duration():
    # Load configuration and database connection
    config = load_config()
    p_connection, p_cursor = connect_to_db(database=config["database"], user=config["user"], password=config["password"], host=config["host"], port=config["port"])

    # Query to select podcasts where duration might be incorrect or unset
    p_cursor.execute("SELECT podcast_episode_id, cache_audio_file, duration FROM podcasts ORDER BY duration;")
    records = p_cursor.fetchall()

    corrupted_files = []
    updated_durations = []

    i=0

    for record in records:
        episode_id, audio_file, old_duration = record

        print(f'[{i}] Computing new duration for:', episode_id, audio_file, old_duration)

        try:
            # Using ffprobe to fetch duration
            result = subprocess.run(
                ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", audio_file],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            new_duration = float(result.stdout.strip())
        except Exception as e:
            print(f"Error processing file {audio_file}: {e}")
            new_duration = -1
            corrupted_files.append(audio_file)

        # Update the database with the new duration or set it to -1 if there was an error
        p_cursor.execute("UPDATE podcasts SET duration = %s WHERE podcast_episode_id = %s;", (new_duration, episode_id))
        p_connection.commit()
        updated_durations.append((audio_file, old_duration if old_duration is not None else -1, new_duration))

        print('Done!',old_duration,'->',new_duration)

        i+=1

    # Log corrupted files with flush after each write
    with open('possibly_corrupted_cache_files.txt', 'w') as file:
        for file_name in corrupted_files:
            file.write(f"{file_name}\n")
            file.flush()

    # Log updated durations with flush after each write
    with open('updated_durations.csv', 'w') as file:
        file.write("filename;old_duration;new_duration\n")
        file.flush()
        for filename, old_duration, new_duration in updated_durations:
            file.write(f"{filename};{old_duration};{new_duration}\n")
            file.flush()

    # Close database connection
    p_cursor.close()
    p_connection.close()

    print("Duration update process completed.")

if __name__ == "__main__":
    update_duration()
