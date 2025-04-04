import feedparser
import wget
import json
import sys
import yaml
import psycopg2
import time
import os
import traceback
import argparse
import subprocess
from langdetect import detect, LangDetectException
from utils import load_config, connect_to_db

p_connection = None
p_cursor = None

# Loads a new line-separated text file with a list of feeds
def load_feeds(list_file):
    with open(list_file) as list_file_in:
        return [elem.rstrip('\n') for elem in list_file_in]

# Creates dir f if it doesn't exist
def ensure_dir(f):
    d = os.path.dirname(f)
    if not os.path.exists(d):
        os.makedirs(d)

# Checks if an audio URL is already in the DB
def check_audio_url(cursor, episode_audio_url):
    cursor.execute('SELECT episode_audio_url, cache_audio_url, cache_audio_file,'
                   'transcript_file from podcasts where episode_audio_url=%s', (episode_audio_url,))
    record = cursor.fetchone()
    if record is not None and len(record) > 0:
        episode_audio_url, cache_audio_url, cache_audio_file, transcript_file = record
        print(f'Skipping, URL already in the database: {episode_audio_url} with {cache_audio_url=} {cache_audio_file=} {transcript_file=}')
        return True
    return False

# Removes tracking links from audio links
def remove_tracking_link(audiolink):
    if audiolink.count('https://') >= 2:
        audiolink = 'https://' + audiolink.split('https://')[-1]
    elif audiolink.count('http://') >= 2:
        audiolink = 'http://' + audiolink.split('http://')[-1]
    return audiolink

# Parses feed feed_url and downloads all episodes
def parse_and_download(feed_url, filter_language):
    d = feedparser.parse(feed_url)

    # If we can't find at least a podcast title, something went wrong or feed_url isn't a RSS feed
    if not 'title' in d.feed:
        print(d.feed)
        print(f'Skipping {feed_url=}, no title found. Maybe not a RSS feed?')
        return

    podcast_title = d.feed['title']

    # Try to find the podcast author
    feed_authors = 'N/A'
    if 'author' in d.feed:
        feed_authors = d.feed['author']

    # Gather meta data for each episode and start downloading
    for episode in d.entries:
        episode_title = episode["title"] if "title" in episode else ''
        desc = episode["description"] if "description" in episode else ''
        published = episode["published"] if "published" in episode else ''
        tags = []
        duration = -1
        link = ''
        audiolink = ''
        mytype = ''

        # Detect language of the description
        try:
            detected_language = detect(desc)
        except LangDetectException:
            detected_language = None

        if filter_language and detected_language != filter_language:
            print(f'Skipping episode "{episode_title}" because its description is not in the specified language ({filter_language}). Detected language: {detected_language}')
            continue

        # Find the audio link in the links section
        for elem in episode["links"]:
            if elem["type"].startswith("audio"):
                mytype = elem["type"]
                audiolink = elem["href"]
            elif elem["type"].startswith("text/html"):
                if not 'href' in elem:
                    print('No href in elem, ignoring episode.', elem)
                    continue
                link = elem["href"]

        # Remove tracking link that makes downloading impossible
        audiolink = remove_tracking_link(audiolink)

        # Add tags (keywords) to list if available
        if 'tags' in episode:
            for tag in episode['tags']:
                tags.append(tag['term'])

        # Get the duration, sometimes it's in seconds, sometimes hh:mm:ss.
        # We convert everything to seconds
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
            elif '.' in duration:
                duration = float(duration)
            else:
                duration = -1
                try:
                    duration = int(duration)
                except:
                    print('Warning, could not parse duration, skipping...')
                    continue
        else:
            print('Warning: no itunes_duration in episode')
        print(episode)

        # Some feeds have episode authors, some don't
        # If available take them, if not, use the overall feed author information
        if "authors" in episode and "name" in episode["authors"]:
            authors = ' '.join(author["name"] for author in episode["authors"])
        else:
            authors = feed_authors

        joined_tags = ', '.join(tags)
        cache_url = ''
        cache_file = ''
        transcript_file = ''
        model_name = ''

        # Use default=str for delta timedelta objects (parsed dates), since the json.dumps function can't handle them otherwise
        episode_json = json.dumps(episode, default=str)

        #print(episode_json)
        print(f"{mytype=}, {episode_title=}, {authors=}, {joined_tags}, {duration=}, {link=}, {audiolink=} {published=}")

        # Only insert into DB if audio URL doesn't already exist in the DB
        if not check_audio_url(p_cursor, episode_audio_url=audiolink):
            # Try to download audiolink and insert into DB if successful
            try:
                audiolink_split = audiolink.split('?')
                assert(len(audiolink_split) <= 2)

                audio_filename = audiolink_split[0].split('/')[-1].replace('%','_')
                assert(len(audio_filename) > 0)

                # Insert unixtime to guarantee that the link is unique
                retrieval_time = time.time()
                unixtime = str(int(retrieval_time))
                cache_file = destination_folder  + '/' + unixtime + '_' + audio_filename
                cache_url = destination_url + '/' + unixtime + '_' + audio_filename

                assert(os.path.exists(cache_file) == False)

                print('Downloading to:', cache_file)
                print('Cache file will be available at:', cache_url)

                subprocess.run([
                    "wget", "--no-check-certificate", "--max-redirect=15",
                    "--retry-connrefused", "--tries=5", "-O", cache_file, audiolink
                ], check=True)

                if os.path.exists(cache_file) and os.path.getsize(cache_file) > 0:
                    file_size_mb = os.path.getsize(cache_file) / (1024 * 1024)
                    print(f'Downloaded file: {cache_file} ({file_size_mb:.2f} MB)')

                    # Prepare the SQL query with placeholders
                    sql = """
                    INSERT INTO podcasts(
                        podcast_title, episode_title, published_date, retrieval_time, authors, language,
                        description, keywords, episode_url, episode_audio_url, cache_audio_url,
                        cache_audio_file, transcript_file, duration, type, episode_json, model
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """

                    # Values to be inserted
                    values = (
                        podcast_title, episode_title, published, str(retrieval_time), authors, language,
                        desc, joined_tags, link, audiolink, cache_url, cache_file, transcript_file,
                        str(duration), mytype, episode_json, model_name
                    )

                    # Print the SQL query with the actual values
                    # print("Executing SQL query:")
                    # print(sql % values)

                    # Execute the SQL query
                    p_cursor.execute(sql, values)
                    p_connection.commit()

                    print("SUCCESS")
                else:
                    print('Error: Downloaded file is empty or does not exist:', cache_file)
            except:
                print('Error occurred while trying to download:', audiolink)
                traceback.print_exc()

    p_connection.commit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser("This is a small utility to download a list of podcast episodes and puts metadata into a PostgreSQL database.")
    parser.add_argument("rss_feed_list", help="List of RSS feeds to process (one per line)")
    parser.add_argument("--language", default='en', help="Language of the podcasts to download (default: 'en')")
    parser.add_argument("--filter-language", default=None, help="Filter podcasts by description language using langdetect (e.g., 'en' for English)")
    args = parser.parse_args()

    rss_feed_list = args.rss_feed_list
    language = args.language
    filter_language = args.filter_language

    config = load_config()

    podcast_language = config["podcast_language"]
    language = podcast_language

    destination_folder = config["download_destination_folder"].replace('{podcast_language}', podcast_language)
    destination_url = config["download_destination_url"].replace('{podcast_language}', podcast_language)

    ensure_dir(destination_folder)
    p_connection, p_cursor = connect_to_db(database=config["database"], user=config["user"],
            password=config["password"], host=config["host"], port=config["port"])

    rss_feeds_in_list = load_feeds(rss_feed_list)

    for feed_url in rss_feeds_in_list:
        print('Downloading from:', feed_url)
        time.sleep(1)
        try:
            parse_and_download(feed_url, filter_language)
        except:
            print('Global and unexpected error occurred while trying to process feed:', feed_url)
            traceback.print_exc()
            print('Warning: will ignore entire feed!')

