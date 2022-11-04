import feedparser
import wget
import json
import sys
import yaml
import psycopg2

test_feed_url = "https://digitalkompakt.podigee.io/feed/mp3"
#test_feed_url = "https://logbuch-netzpolitik.de/feed/opus"

language = 'de'
destination_folder = f'/var/www/speechcatcher.net/cache/podcasts/{language}/'

p_connection = None
p_cursor = None

def load_config(config_filename='config.yaml'):
    with open("config.yaml", "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
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
        sys.exit(-1)

def parse_and_download(feed_url):
    d = feedparser.parse(test_feed_url)

    podcast_title = d.feed['title']

    for episode in d.entries:

        episode_title = episode["title"]    
        desc = episode["description"]
        published = episode["published"]
        tags = []
        duration = -1
        link = ''

        # find the audio link in the links section
        for elem in episode["links"]:
            if elem["type"].startswith("audio"):
                mytype = elem["type"]
                link = elem["href"]
        
        # add tags (keywords) to list if available
        if 'tags' in episode: 
            for tag in episode['tags']:
                tags.append(tag['term'])

        # get the duration, sometimes its in seconds sometimes hh:mm:ss.
        # we convert everything to seconds
        if 'itunes_duration' in episode:
            duration = episode['itunes_duration']

            if ':' in duration:
                dur_split = duration.split(':')
                assert(len(dur_split) == 3)
                duration = int(dur_split[0])*3600 + int(dur_split[1])*60 + int(dur_split[2])
            else:
                duration = int(duration)
        else:
            print('Warning: no itunes_duration in episode')
        
        authors = ' '.join(author["name"] for author in episode["authors"])
        joined_tags = ', '.join(tags)
        cache_url = ''
        cache_file = ''
        transcript_file = ''

        # delte timedelta objects (parsed dates), since the json.dumps function can't handle them
        episode_json = json.dumps(episode, default=str) 

        print(f"{mytype=}, {episode_title=}, {authors=}, {joined_tags}, {duration=}, {link=}, {published=}")

        # CREATE TABLE IF NOT EXISTS podcasts (podcast_episode_id serial PRIMARY KEY, podcast_title TEXT, episode_title TEXT, authors TEXT, language VARCHAR(16), description TEXT, keywords TEXT, episode_url TEXT, cache_url TEXT, cache_file TEXT, transcript_file TEXT, duration REAL, type VARCHAR(64), episode_json JSON);


        sql = c.execute("INSERT INTO podcasts VALUES (DEFAULT,?,?,?,?,?,?,?)", (podcast_title, episode_title, authors, language, desc, joined_tags, cache_url, transcript_file, float(duration), mytype, episode_json))
        p_cursor.execute(sql)

if __name__ == "__main__":
    config = load_config()
    p_connection, p_cursor = connect_to_db(database=config["database"], user=config["user"], password=config["password"], host=config["host"], port=config["port"])
    parse_and_download(test_feed_url)
