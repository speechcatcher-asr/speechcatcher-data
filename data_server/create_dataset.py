# Create a dataset (Kaldi format) with the server.py API for podcasts.
# The dataset is divided into train/dev/test.
# Timestamps from the vtt files are used for the segments.

import requests
import random
import argparse
import hashlib
import re
import traceback
import concurrent.futures
import sys
import os
import json
import unicodedata
from utils import *

# You can also use sox, but fileformats are more limited.
sox_str = '%s sox %s -t wav -r 16k -b 16 -e signed -c 1 - |\n'

# With ffmpeg, the dataset can load any file and can convert it to 16kHz wav on-the-fly.
sox_str = '%s ffmpeg -i "%s" -acodec pcm_s16le -ar 16000 -ac 1 -f wav - |\n'

ex_file_path = 'exclusion_chars/{lang}.txt'

def create_exclusion_dict(ex_file_path):
    exclusion_dict = {}
    with open(ex_file_path, "r") as f:
        for line in f:
            char = line.strip()
            exclusion_dict[char] = True
    return exclusion_dict

def check_exclusion(string, exclusion_dict):
    return any(exclusion_dict.get(char, False) for char in string)

def check_exclusion_reason(string, exclusion_dict):
    excluded_chars = {char for char in string if char in exclusion_dict}
    if excluded_chars:
        return f"The text contained these exclusion characters: {', '.join(excluded_chars)}"
    return "The text is valid."

class InvalidURLException(Exception):
    """Exception raised for invalid URL or file path."""
    def __init__(self, url, message="The URL or file path is invalid"):
        self.url = url
        self.message = message
        super().__init__(self.message)

def read_local_file(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No file found at {file_path}")
    with open(file_path, 'r') as file:
        return file.read()

# Converts a vtt timestamp string to float (in seconds)
# Detects timestamps as well that do not prefix hours
# examples:
# 00:59.999 -> 59.999
# 05:36.450 -> 336.45
# 01:23:45.678 -> 5025.678

def timestamp_to_seconds_float(str_timestamp):
    time_parts = re.split(r':|\.', str_timestamp)

    if len(time_parts[-1]) == 3:
        milliseconds_div = 1000.
    elif len(time_parts[-1]) == 6:
        milliseconds_div = 1000000.
    else:
        raise ValueError("Invalid timestamp format (cant figure out milisecond format):",str_timestamp)

    if len(time_parts) == 4:
        hours, minutes, seconds, milliseconds = [float(time_part) for time_part in time_parts]
        return (hours * 3600.) + (minutes * 60.) + seconds + (milliseconds / milliseconds_div)
    elif len(time_parts) == 3:
        minutes, seconds, milliseconds = [float(time_part) for time_part in time_parts]
        return (minutes * 60.) + seconds + (milliseconds / milliseconds_div)
    else:
        raise ValueError("Invalid timestamp format (cant convert to float):",str_timestamp)

def is_printable_unicode(char):
    category = unicodedata.category(char)
    # Categories starting with 'C' are control chars, format chars, etc.
    return not category.startswith('C')

def find_non_printable_unicode_lines(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, start=1):
            if line[-1] == '\n':
                line = line[:-1]
            non_printables = [(i, c, unicodedata.name(c, "UNKNOWN"), f"U+{ord(c):04X}")
                              for i, c in enumerate(line)
                              if not is_printable_unicode(c)]
            if non_printables:
                print(f"Line {line_num} has non-printable Unicode characters:")
                print(f"  Text: {line.strip()}")
                for idx, char, name, code in non_printables:
                    print(f"    - Pos {idx}: '{char}' ({name}, {code})")

# a few problematic unicode characters that we replace with ASCII chars 
CHAR_REPLACEMENTS = {
    '\u200b': ' ',   # ZERO WIDTH SPACE
    '\u200c': ' ',   # ZERO WIDTH NON-JOINER
    '\u202c': ' ',   # POP DIRECTIONAL FORMATTING
    '\u00ad': '-',   # SOFT HYPHEN
    '\u0007': ' ',   # BELL character
    '\u200e': ' ',   # LEFT-TO-RIGHT MARK
    '\ue000': ' ',   # PRIVATE USE AREA CHAR
}

def clean_line(line):
    cleaned = []
    for c in line:
        if c == '\n':
            cleaned.append(c)  # preserve newline
        elif c in CHAR_REPLACEMENTS:
            cleaned.append(CHAR_REPLACEMENTS[c])
        elif unicodedata.category(c).startswith('C'):
            cleaned.append(' ')
        else:
            cleaned.append(c)
    return ''.join(cleaned)

# Write out a dataset of episodes to <dataset_dir>
# podcasts is a list of podcasts, where a podcast has the following structure:
# podcast = {'title': str, 'episodes': list of episodes}
#                           |
#                           episode = {'transcript_file': str, 'segments': list of segments, 'authors': str}
#                                                               |
#                                                               segment = {'text': str, 'start': str, 'end': str}
# The start and end time stamps should already be converted to Kaldi format, i.e. decimals in seconds (as a string), see above timestamp_to_seconds_float function.
#
# We derive a sha1 hash from the filename as episode id and from author+podcast as author id (first 20 chars). Note that the final utterance must be prefixed by the speaker id:
# > The main assumption is that the sorting order of utt2spk will stay the same, independently whether you will sort by speaker or utterance. We suggest making the utterances to be prefixed by the speaker ids -- that should resolve your issues
# see https://groups.google.com/g/kaldi-help/c/n8es2XWVkec?pli=1

def write_kaldi_dataset(podcasts, dataset_dir, use_sox_str=True, remove_non_printable_utterances=False):
    ensure_dir(dataset_dir)
    with open(f'{dataset_dir}/text', 'w') as text_file, \
         open(f'{dataset_dir}/segments', 'w') as segments_file, \
         open(f'{dataset_dir}/utt2spk', 'w') as utt2spk_file, \
         open(f'{dataset_dir}/wav.scp', 'w') as wav_scp_file, \
         open(f'{dataset_dir}/id2podcast.tsv', 'w') as id2podcast_file, \
         open(f'{dataset_dir}/utt2dur', 'w') as utt2dur_file:
      for podcast in podcasts:
          for episode in podcast['episodes']:
              try:
                  filename = episode['cache_audio_file']
                  timestamp = get_duration(filename)
                  max_seconds = timestamp_to_seconds_float(timestamp)
                  print(filename, 'max_seconds:', max_seconds, 'timestamp', timestamp)
              except:
                  print('Couldnt get duration from', filename, 'warning: ignoring entire file.')
                  continue

              vtt_file = episode['transcript_file']

              if '/corrupted/' in vtt_file:
                  print(vtt_file, 'vtt file is corrputed, skipping!')
                  continue

              if not episode['segments']:
                  print(vtt_file, 'no segments in vtt transcript (after filtering), skipping!')
                  continue

              author = episode['authors'] + '_' + podcast['title']
              episode_id = hashlib.sha1(filename.encode()).hexdigest()[:20]
              speaker_id = hashlib.sha1(author.encode()).hexdigest()[:20]
              recording_id = f'{speaker_id}_{episode_id}'

              if use_sox_str:
                  wav_scp_file.write(sox_str % (recording_id, filename))
              else:
                  wav_scp_file.write(f'{recording_id} {filename}\n')
              id2podcast_file.write(f'{recording_id}\t{podcast["title"]}\n')
              utt2dur_file.write(f'{recording_id} {max_seconds}\n')

              for i, segment in enumerate(episode['segments']):
                  #convert timestamps if nessecary
                  start = segment['start'] if isinstance(segment['start'], float) else timestamp_to_seconds_float(segment['start'])
                  end = segment['end'] if isinstance(segment['end'], float) else timestamp_to_seconds_float(segment['end'])

                  if start > max_seconds:
                      print(f'Warning, overflow in transcript for start time stamp for {filename}... ignore and skip this and the following segments.')
                      break

                  if end > max_seconds:
                      print(f'Warning, overflow in transcript end time stamp for {filename}... trying to fix.')
                      end = max_seconds

                  if end <= start:
                      print(f'End timestamp now underflows start, ignoring entire segment')
                      break

                  text = segment['text']

                  # skip if text contains a bogus char
                  if check_exclusion(text, exclusion_dict):
                      print(f'Exclusion character found, ignoring entire segment. Text is:', text)
                      print(check_exclusion_reason(text, exclusion_dict))
                      continue

                  # Check for non-printable Unicode characters
                  if remove_non_printable_utterances and not all(is_printable_unicode(c) or c == '\n' for c in text):
                      print(f'Non-printable Unicode character found, ignoring entire segment. Text is:', text)
                      continue

                  # Clean the text
                  text = clean_line(text)

                  recording_id = f'{speaker_id}_{episode_id}'
                  utterance_id = f'{speaker_id}_{episode_id}_{"%.7d" % i}'

                  # format of the segments file is: <utterance-id> <recording-id> <segment-begin> <segment-end>
                  segments_file.write(f'{utterance_id} {recording_id} {start} {end}\n')
                  # format of the text file is: <utterance-id> <text>
                  text_file.write(f'{utterance_id} {text}\n')
                  # format of the utt2spk file is: <utterance-id> <speaker-id>
                  utt2spk_file.write(f'{utterance_id} {speaker_id}\n')

    print('Wrote Kaldi/Espnet dataset to:', dataset_dir)

# This joins consecutive segments at random, up to a specified max length.
# The output segment list is shortened and the segments are longer.
def join_consecutive_segments_randomly(segments, max_length=15):

    segments_copy = segments.copy()
    joined_segments = []

    i = 0
    max_i = len(segments_copy)
    while i < len(segments_copy):
        num_segments_to_merge = random.randint(1, max_length)

        if i+num_segments_to_merge > max_i:
          num_segments_to_merge = max_i - i

        if num_segments_to_merge == 0:
            break

        # Merge the chosen number of segments
        segment_text = ' '.join([segment['text'] for segment in segments_copy[i:i+num_segments_to_merge]])

        joined_segments.append({
            'start': segments_copy[i]['start'],
            'end': segments_copy[i+num_segments_to_merge-1]['end'],
            'text': segment_text
        })

        i += num_segments_to_merge

    return joined_segments

# Download the transcript file as text/string
def download_file(file_url):
    response = requests.get(file_url)
    return response.text

# Load and parse a JSON file to extract timestamps and text
def parse_json_segments(json_content):
    data = json.loads(json_content)

    segments = []
    for segment in data.get("segments", []):
        segments.append({
            'start': segment["start"],
            'end': segment["end"],
            'text': segment["text"]
        })

    return segments

# Parse a VTT file and extract timestamps and text
# Any segment that repeats more often than ignore_repeat_lines times will be ignored (very probable whisper hallucination)
def parse_vtt_segments(vtt_content, ignore_repeat_lines=3):
    lines = vtt_content.split('\n')
    segments = []

    # Iterate over the lines and parse the segments
    current_segment = None
    last_text = None
    repeat_count = 0

    for line in lines:
        if line.startswith('WEB'):
            continue

        # This line indicates the start of a new segment and time stamp info
        if '-->' in line:
            if current_segment:
                current_text = current_segment['text'].strip()
                repeat_count = repeat_count + 1 if current_text == last_text else 0

                if repeat_count < ignore_repeat_lines:
                    segments.append(current_segment)
                    last_text = current_text
            # Start a new segment
            a, b = line.split('-->')
            a, b = a.strip(), b.strip()
            current_segment = {'start': a, 'end': b, 'text': ''}
        elif line.strip():
            if current_segment['text'] == '':
                current_segment['text'] = line
            else:
                current_segment['text'] += '\n' + line

    # Handle the last segment
    if current_segment:
        current_text = current_segment['text'].strip()
        repeat_count = repeat_count + 1 if current_text == last_text else 0

        if repeat_count < ignore_repeat_lines:
            segments.append(current_segment)

    return segments

# process_podcast wrapper to catch exceptions in process_podcast
def process_podcast_wrapper(server_api_url, api_secret_key, elem_title, audio_dataset_location, replace_audio_dataset_location, change_audio_fileending, file_format):
    try:
        return process_podcast(server_api_url, api_secret_key, elem_title, audio_dataset_location, replace_audio_dataset_location, change_audio_fileending, file_format)
    except:
        print('Warning: error in ', elem_title, 'ignoring entire podcast...')
        traceback.print_exc()

# Process all episodes of a particular podcast
def process_podcast(server_api_url, api_secret_key, title, audio_dataset_location='', replace_audio_dataset_location='', change_audio_fileending='', file_format='vtt'):

    request_url = f"{server_api_url}/get_episode_list/{api_secret_key}"
    data = {'podcast_title': title}

    print('server_api_url:', request_url)

    response = requests.post(request_url, data=data, timeout=120)

    episode_list = response.json()

    #print(episode_list)

    episodes = []

    for episode in episode_list:
        if 'episode_title' not in episode:
            print('WARNING: Malformed episode (skipping):', episode)
            continue
        try:
            print('parsing:', episode['episode_title'])

            # ignore in_progress and empty urls
            if episode['transcript_file_url']=='in_progress':
                print('Warning, ignoring in_progress episode url.')
                continue

            if episode['transcript_file_url']=='':
                print('Warning, ignoring empty episode url.')
                continue

            file_content = None
            url = episode['transcript_file_url']

            if url.startswith('http'):
                file_content = download_file(url)
            elif url.startswith('/'):
                file_content = read_local_file(url)
            else:
                raise InvalidURLException(url)

            # If replace_audio_dataset_location isn't empty, change the server reported (absolute) filenames.
            # This is useful if you store the dataset on different servers in different directories, e.g.
            # to change all filenames containing /var/www -> /srv (replace_audio_dataset_location='/var/www', audio_dataset_location='/srv')

            if replace_audio_dataset_location != '':
                episode['cache_audio_file'] = episode['cache_audio_file'].replace(replace_audio_dataset_location, audio_dataset_location)
                episode['transcript_file'] = episode['transcript_file'].replace(replace_audio_dataset_location, audio_dataset_location)

            if change_audio_fileending != '':
                if episode['cache_audio_file'].endswith('.mp3'):
                    episode['cache_audio_file'] = episode['cache_audio_file'][:-4] + change_audio_fileending
                elif episode['cache_audio_file'].endswith('.opus'):
                    episode['cache_audio_file'] = episode['cache_audio_file'][:-5] + change_audio_fileending

            if file_format == 'vtt':
                segments = parse_vtt_segments(file_content)
            elif file_format == 'json':
                segments = parse_json_segments(file_content)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")

            segments_merged = join_consecutive_segments_randomly(segments)

            episode_copy = episode.copy()
            episode_copy['segments'] = segments_merged

            episodes.append(episode_copy)
        except:
            print('Error processing episode:', episode, 'skipping...')
            traceback.print_exc()

    return {'title': title, 'episodes': episodes}

# Divide dataset into train/dev/test and start processing the podcasts
def process(server_api_url, api_secret_key, dev_n=10, test_n=10, test_dev_episodes_threshold=10, language='en',
                                     audio_dataset_location='', replace_audio_dataset_location='', change_audio_fileending='', file_format='vtt', remove_non_printable_utterances=False):

    request_url = f"{server_api_url}/get_podcast_list/{language}/{api_secret_key}"
    response = requests.get(request_url)
    podcast_list = response.json()

    print('Number of podcasts:', len(podcast_list))
    print('Dev_n:', dev_n)
    print('Test_n:', test_n)
    print('test_dev_episodes_threshold:', test_dev_episodes_threshold)

    podcast_list_test_dev_pool = [podcast for podcast in podcast_list if (podcast['count'] < test_dev_episodes_threshold)]

    dev_set = random.sample(podcast_list_test_dev_pool, dev_n)
    podcast_list_test_dev_pool = [x for x in podcast_list_test_dev_pool if x not in dev_set]

    test_set = random.sample(podcast_list_test_dev_pool, test_n)

    train_set = [x for x in podcast_list if (x not in dev_set) and (x not in test_set)]

    #print(dev_set)
    #print(test_set)

    # create dev set in parallel
    dev_podcasts = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_podcast, server_api_url, api_secret_key, elem['title'], audio_dataset_location, replace_audio_dataset_location, change_audio_fileending, file_format) for elem in dev_set]

        # Use the as_completed() function to iterate over the completed futures and retrieve their results
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            dev_podcasts.append(result)

    write_kaldi_dataset(dev_podcasts, 'data/dev/', remove_non_printable_utterances=remove_non_printable_utterances)

    # create test set in parallel
    test_podcasts = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_podcast, server_api_url, api_secret_key, elem['title'], audio_dataset_location, replace_audio_dataset_location, change_audio_fileending, file_format) for elem in test_set]

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            test_podcasts.append(result)

    write_kaldi_dataset(test_podcasts, 'data/test/', remove_non_printable_utterances=remove_non_printable_utterances)

    # create train set in parallel
    train_podcasts = []

    with concurrent.futures.ProcessPoolExecutor() as executor:
        podcast_futures = [executor.submit(process_podcast_wrapper, server_api_url, api_secret_key, elem['title'],
                           audio_dataset_location, replace_audio_dataset_location, change_audio_fileending, file_format) for elem in train_set]
        try:
            for future in concurrent.futures.as_completed(podcast_futures):
                podcast = future.result()
                if podcast is not None:
                    train_podcasts.append(podcast)
        except KeyboardInterrupt:
            print('User abort: Cancelling remaining tasks')
            for future in podcast_futures:
                future.cancel()
            concurrent.futures.wait(podcast_futures)
            sys.exit(-1)
    write_kaldi_dataset(train_podcasts, 'data/train/', remove_non_printable_utterances=remove_non_printable_utterances)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create a dataset (Kaldi format) with the server.py API')
    parser.add_argument('-d', '--dev', default=10, dest='dev_n', help='Sample dev set from n speakers/podcasts', type=int)
    parser.add_argument('-t', '--test', default=10, dest='test_n', help='Sample test set from n speakers/podcasts', type=int)
    parser.add_argument('-n', '--test_dev_episodes_threshold', default=10, dest='test_dev_episodes_threshold',
      help='Only sample the test and dev set from shorter podcasts, where len(episodes) is smaller than this value', type=int)
    parser.add_argument('--file-format', choices=['vtt', 'json'], default='vtt', help='Specify the subtitle file format (vtt or json)')
    parser.add_argument('--debug', dest='debug', help='Start with debugging enabled',
                        action='store_true', default=False)
    parser.add_argument('--remove-non-printable-utterances', dest='remove_non_printable_utterances', help='Remove utterances with non-printable Unicode characters',
                        action='store_true', default=False)
    parser.add_argument('-y', '--yes', dest='auto_confirm', help='Bypass the confirmation prompt',
                                            action='store_true', default=False)

    args = parser.parse_args()

    random.seed(42)
    config = load_config()
    api_secret_key = config["secret_api_key"]
    server_api_url = config["server_api_url"]
    audio_dataset_location = config["audio_dataset_location"]
    replace_audio_dataset_location = config["replace_audio_dataset_location"]
    change_audio_fileending = config["change_audio_fileending_to"]
    language = config["podcast_language"]
    file_format = args.file_format
    ex_file_path_lang = ex_file_path.replace('{lang}', language)

    # Print configuration summary
    print("\nConfiguration Summary:")
    print(f"API Secret Key: {'*' * len(api_secret_key)} (hidden for security)")
    print(f"Server API URL: {server_api_url}")
    print(f"Audio Dataset Location: {audio_dataset_location}")
    print(f"Replace Audio Dataset Location: {replace_audio_dataset_location}")
    print(f"Change Audio File Ending To: {change_audio_fileending}")
    print(f"Podcast Language: {language}")
    print(f"Exclusion character list: {ex_file_path_lang}")
    print(f"File format: {file_format}")
    print(f"Remove non-printable utterances: {args.remove_non_printable_utterances}")

    # Confirm before proceeding
    if not args.auto_confirm:
        confirm = input("\nDo you want to proceed with dataset creation? (y/n): ").strip().lower()
        if confirm != 'y':
            print("Aborting dataset creation.")
            sys.exit(-1)

    exclusion_dict = create_exclusion_dict(ex_file_path_lang)

    process(server_api_url, api_secret_key, args.dev_n, args.test_n, args.test_dev_episodes_threshold, language,
            audio_dataset_location, replace_audio_dataset_location, change_audio_fileending, file_format=file_format, remove_non_printable_utterances=args.remove_non_printable_utterances)

