import yaml
import psycopg2
import traceback
import os
import sys
import subprocess

# inspired by https://stackoverflow.com/questions/31024968/using-ffmpeg-to-obtain-video-durations-in-python
def get_duration(input_video):
    cmd = ['ffprobe', '-i', input_video, '-show_entries', 'format=duration', '-v', 'quiet', '-sexagesimal', '-of', 'csv=p=0']
    return subprocess.check_output(cmd).decode("utf-8").strip()

def ensure_dir(f):
    d = os.path.dirname(f)
    if not os.path.exists(d):
        os.makedirs(d)

def load_config(config_filename='../config.yaml'):
    with open(config_filename, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            traceback.print_exc()
            sys.exit(-3)

def connect_to_db(database, user, password, host='127.0.0.1', port='5432'):
    # Connect to DB
    try:
        mct_connection = psycopg2.connect(user = user,
                                      password = password,
                                      host = host,
                                      port = port,
                                      database = database)

        mct_cursor = mct_connection.cursor()

        # Print PostgreSQL version
        mct_cursor.execute("SELECT version();")
        record = mct_cursor.fetchone()
        print("You are connected to Postgres - ", record,"\n")

        return mct_connection, mct_cursor

    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
        traceback.print_exc()
        sys.exit(-1)
 
