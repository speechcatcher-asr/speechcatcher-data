import feedparser
import wget
import json
import sys
import yaml
import psycopg2
import time
import os
import traceback
from utils import load_config, connect_to_db 

# TODO: make these configurable
language = 'de'
rss_feed_list = 'rss_feeds_de2'

destination_folder = f'/var/www/speechcatcher.net/cache/podcasts/{language}'
destination_url = f'https://speechcatcher.net/cache/podcasts/{language}'

p_connection = None
p_cursor = None

def load_feeds(list_file):
    with open(list_file) as list_file_in:
        return [elem.rstrip('\n') for elem in list_file_in]

def ensure_dir(f):
    d = os.path.dirname(f)
    if not os.path.exists(d):
        os.makedirs(d)

def check_audio_url(cursor, episode_audio_url):
    cursor.execute('SELECT episode_audio_url, cache_audio_url, cache_audio_file,'\
                   'transcript_file from podcasts where episode_audio_url=%s', (episode_audio_url,) )
    record = cursor.fetchone()
    if record is not None and len(record) > 0:
        episode_audio_url, cache_audio_url, cache_audio_file, transcript_file = record
        print(f'Skipping, URL already in the database: {episode_audio_url} with {cache_audio_url=} {cache_audio_file=} {transcript_file=}')
        return True
    return False

# 'https://chtbl.com/track/E43E46/https://lcdn.letscast.fm/media/podcast/62368df4/episode/3b2c0786.mp3?t=1618380826' 
def remove_tracking_link(audiolink):
    if audiolink.count('https://') >= 2:
        audiolink = 'https://' + audiolink.split('https://')[-1]
    elif audiolink.count('http://') >= 2:
        audiolink = 'http://' + audiolink.split('http://')[-1]
    return audiolink

def parse_and_download(feed_url):
    d = feedparser.parse(feed_url)

    podcast_title = d.feed['title']

    feed_authors = 'N/A'
    if 'author' in d.feed:
        feed_authors = d.feed['author']

    for episode in d.entries:

        episode_title = episode["title"]    
        desc = episode["description"]
        published = episode["published"]
        tags = []
        duration = -1
        link = ''
        audiolink = ''

        # find the audio link in the links section
        for elem in episode["links"]:
            if elem["type"].startswith("audio"):
                mytype = elem["type"]
                audiolink = elem["href"]
            elif elem["type"].startswith("text/html"):
                link = elem["href"]

        # remove tracking link that makes downloading impossible
        audiolink = remove_tracking_link(audiolink)

        # add tags (keywords) to list if available
        if 'tags' in episode: 
            for tag in episode['tags']:
                tags.append(tag['term'])

        # get the duration, sometimes its in seconds sometimes hh:mm:ss.
        # we convert everything to seconds
        if 'itunes_duration' in episode:
            duration = episode['itunes_duration']

            if duration == '':
                print('Warning, skipping this episode since duration is empty.')
                continue

            if ':' in duration:
                dur_split = duration.split(':')
                assert(len(dur_split) <= 3)
                if len(dur_split) == 3: # hh:mm:ss
                    duration = int(dur_split[0])*3600 + int(dur_split[1])*60 + int(dur_split[2])
                elif len(dur_split) == 2: # mm:ss
                    duration = int(dur_split[0])*60 + int(dur_split[1])
            else:
                duration = int(duration)
        else:
            print('Warning: no itunes_duration in episode')
        print(episode) 

        # some feeds have episode authors, some don't
        # if available take them, if not, use the overall feed author information
        if "authors" in episode:
            authors = ' '.join(author["name"] for author in episode["authors"])
        else:
            authors = feed_authors

        joined_tags = ', '.join(tags)
        cache_url = ''
        cache_file = ''
        transcript_file = ''

        # Use default=str for delta timedelta objects (parsed dates), since the json.dumps function can't handle them otherwise
        episode_json = json.dumps(episode, default=str) 

        #print(episode_json)
        print(f"{mytype=}, {episode_title=}, {authors=}, {joined_tags}, {duration=}, {link=}, {audiolink=} {published=}")

        # Only insert into DB if audio URL doesn't already exist in the DB
        if not check_audio_url(p_cursor, episode_audio_url=audiolink):
            # Try to download audiolink and insert into db if succesful
            try:
                audiolink_split = audiolink.split('?')
                assert(len(audiolink_split) <= 2)
                
                audio_filename = audiolink_split[0].split('/')[-1]
                assert(len(audio_filename) > 0)

                # insert unixtime to guarantee that the link is unique
                retrieval_time = time.time()
                unixtime = str(int(retrieval_time))
                cache_file = destination_folder  + '/' + unixtime + '_' + audio_filename
                cache_url = destination_url + '/' + unixtime + '_' + audio_filename 
        
                assert(os.path.exists(cache_file) == False)

                print('Downloading to:', cache_file)
                print('Cache file will be available at:', cache_url)
                wget.download(audiolink, out=cache_file, bar=wget.bar_thermometer)
                print()

                sql = "INSERT INTO podcasts(podcast_title, episode_title, published_date, retrieval_time, authors, language, description, keywords, episode_url, episode_audio_url," \
                  " cache_audio_url, cache_audio_file, transcript_file, duration, type, episode_json) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                p_cursor.execute(sql, (podcast_title, episode_title, published, str(retrieval_time), authors, language, desc, joined_tags, link, audiolink, cache_url, cache_file, transcript_file, str(duration), mytype, episode_json))
                p_connection.commit()
            except:
                print('Error occured while trying to download:', audiolink)
                traceback.print_exc()

    p_connection.commit()

if __name__ == "__main__":
    config = load_config()
    ensure_dir(destination_folder)
    p_connection, p_cursor = connect_to_db(database=config["database"], user=config["user"], password=config["password"], host=config["host"], port=config["port"])
    
    rss_feeds_de_list = load_feeds(rss_feed_list)

    for feed_url in rss_feeds_de_list:
        print('Downloading from:', feed_url)
        time.sleep(1)
        parse_and_download(feed_url)
