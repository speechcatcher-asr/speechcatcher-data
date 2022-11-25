import yaml
import psycopg2
import time
import os
import traceback
import pickle

from utils import load_config, connect_to_db

webpage = '/var/www/speechcatcher.net/stats.html'
podcast_table = 'podcasts'

config = load_config()
p_connection, p_cursor = connect_to_db(database=config["database"], user=config["user"], password=config["password"], host=config["host"], port=config["port"])

# podcast table schema:
#CREATE TABLE IF NOT EXISTS podcasts (podcast_episode_id serial PRIMARY KEY, podcast_title TEXT, episode_title TEXT, published_date TEXT, retrieval_time DECIMAL, authors TEXT, language VARCHAR(16), description TEXT, keywords TEXT, episode_url TEXT, episode_audio_url TEXT, cache_audio_url TEXT, cache_audio_file TEXT, transcript_file TEXT, duration REAL, type VARCHAR(64), episode_json JSON);

# load untranscribed and transcribed sums from database
sql = "select sum(duration) from " + podcast_table + " where transcript_file <> '' and transcript_file <> 'in_progress';" 

p_cursor.execute(sql)
record = p_cursor.fetchone()[0]
if record is None:
    transcribed_hours = 0.
else:
    transcribed_hours = float(record) / 3600.

sql = "select sum(duration) from " + podcast_table + " where transcript_file = '' or transcript_file = 'in_progress';"

p_cursor.execute(sql)
record = p_cursor.fetchone()[0]
untranscribed_hours = float(record) / 3600.

transcribed_ratio = transcribed_hours / (untranscribed_hours+transcribed_hours)

# in progress

sql = "select sum(duration) from " + podcast_table + " where transcript_file = 'in_progress';"

p_cursor.execute(sql)
record = p_cursor.fetchone()[0]

if record is None:
    inprogress_hours = 0.
else:
    inprogress_hours = float(record) / 3600.

# estimate transcription speed
try:
    with open('html_stats.pickle',"rb") as pickle_f:
        prev_time, prev_transcribed_hours, prev_untranscribed_hours, prev_total = pickle.load(pickle_f)
except:
    print('Couldnt load html_stats.pickle')
    prev_time, prev_transcribed_hours, prev_untranscribed_hours, prev_total = 0.,0.,0.,0.

transcription_speed = 0.
if prev_time!= 0.:
    time_interval = time.time() - prev_time
    time_interval_in_hours = time_interval / 3600.
    transcription_speed = (transcribed_hours - prev_transcribed_hours) / time_interval_in_hours

print(f'{transcribed_hours=}', f'{untranscribed_hours=}', f'{inprogress_hours=}' , f'{transcribed_ratio=}', f'{transcription_speed=}')

html = f'''<html>
<head><title>Speechcatcher dataset stats</title></head
<body>

<h1>Speechcatcher dataset stats</h1>

<p>Transcribed {round(transcribed_hours, 2)} hours from {round(untranscribed_hours+transcribed_hours, 2)} hours in total ({round(transcribed_ratio*100.0,2)}%).</p>

<svg width="450" height="20">
  <rect width="400" height="15" style="fill:grey" />
  <rect width="{int(transcribed_ratio * 400)}" height="15" style="fill:green" />
</svg>
   
<p>Current transcription speed is: {round(transcription_speed, 2)} hours per hour</p>
<p>Currently in progress: {round(inprogress_hours, 2)} hours </p>
</body>
</html>'''

with open(webpage,'w') as out_file:
    out_file.write(html)

with open('html_stats.pickle',"wb") as pickle_f:
    pickle.dump([time.time(), transcribed_hours, untranscribed_hours, untranscribed_hours+transcribed_hours], pickle_f)
