# Create a dataset (Kaldi format) with the server.py API for podcasts.
# The dataset is divided into train/dev/test.
# Timestamps from the vtt files are used for the segments.

import requests
import random
import argparse
import hashlib
import re
import traceback
from utils import *

# Converts a vtt timestamp string to float (in seconds)
# examples:
# 00:59.999 -> 59.999
# 05:36.450 -> 336.45
# 01:23:45.678 -> 5025.678

def timestamp_to_seconds_float(str_timestamp):
    time_parts = re.split(':|\.', str_timestamp)
    
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

def write_kaldi_dataset(podcasts, dataset_dir):
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

              author = episode['authors'] + '_' + podcast['title']
              episode_id = hashlib.sha1(filename.encode()).hexdigest()[:20]
              speaker_id = hashlib.sha1(author.encode()).hexdigest()[:20]
              recording_id = f'{speaker_id}_{episode_id}'

              wav_scp_file.write(f'{recording_id} {filename}\n')
              id2podcast_file.write(f'{recording_id}\t{podcast["title"]}\n')
              utt2dur_file.write(f'{recording_id} {max_seconds}\n')

              for i, segment in enumerate(episode['segments']):
                  start = timestamp_to_seconds_float(segment['start'])
                  end = timestamp_to_seconds_float(segment['end'])

                  if start > max_seconds:
                      print(f'Warning, overflow in vtt for start time stamp for {filename}... ignore and skip this and the following segments.')
                      break

                  if end > max_seconds:
                      print(f'Warning, overflow in vtt end time stamp for {filename}... trying to fix.')
                      end = max_seconds

                  if end < start:
                      print(f'End timestamp now underflows start, ignoring entire segment')
                      break

                  text = segment['text']
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

# Download the VTT file
def download_vtt_file(vtt_file_url):
    response = requests.get(vtt_file_url)
    vtt_content = response.text

    return vtt_content

# Parse a VTT file and extract timestamps and text
def parse_vtt_segments(vtt_content):
    lines = vtt_content.split('\n')
    segments = []

    # Iterate over the lines and parse the segments
    current_segment = None
    for line in lines:
        if line.startswith('WEB'):
            continue
        # This line indicates the start of a new segment and time stamp info
        if '-->' in line:
            if current_segment:
                segments.append(current_segment)
            a,b = line.split('-->')
            a,b = a.strip(), b.strip()
            current_segment = {'start': a, 'end': b, 'text':''}
        elif line.strip():
            if current_segment['text'] == '':
                current_segment['text'] = line
            else:
                current_segment['text'] += '\n' + line

    # Add the last segment
    if current_segment:
        segments.append(current_segment)

    return segments

# Process all episodes of a particular podcast
def process_podcast(server_api_url, api_secret_key, title, audio_dataset_location='', replace_audio_dataset_location=''):

    request_url = f"{server_api_url}/get_episode_list/{api_secret_key}"
    data = {'podcast_title': title}

    print('server_api_url:', request_url)

    response = requests.post(request_url, data=data, timeout=120)

    episode_list = response.json()

    #print(episode_list)

    episodes = []

    for episode in episode_list:
        try:
            print('parsing:', episode['episode_title'])

            # ignore in_progress and empty urls
            if episode['transcript_file_url']=='in_progress':
                print('Warning, ignoring in_progress episode url.')
                continue

            if episode['transcript_file_url']=='':
                print('Warning, ignoring empty episode url.')
                continue

            vtt_content = download_vtt_file(episode['transcript_file_url'])

            # If replace_audio_dataset_location isn't empty, change the server reported (absolute) filenames.
            # This is useful if you store the dataset on different servers in different directories, e.g.
            # to change all filenames containing /var/www -> /srv (replace_audio_dataset_location='/var/www', audio_dataset_location='/srv')
            
            if replace_audio_dataset_location != '':
                episode['cache_audio_file'] = episode['cache_audio_file'].replace(replace_audio_dataset_location, audio_dataset_location)
                episode['transcript_file'] = episode['transcript_file'].replace(replace_audio_dataset_location, audio_dataset_location)

            segments = parse_vtt_segments(vtt_content) 
            segments_merged = join_consecutive_segments_randomly(segments)
            
            episode_copy = episode.copy()
            episode_copy['segments'] = segments_merged

            episodes.append(episode_copy)
        except:
            print('Error processing episode:', episode['episode_title'],'skipping...')
            traceback.print_exc()

    return {'title': title, 'episodes': episodes}

# Divide dataset into train/dev/test and start processing the podcasts
def process(server_api_url, api_secret_key, dev_n=10, test_n=10, test_dev_episodes_threshold=10,
                                     audio_dataset_location='', replace_audio_dataset_location=''):
    
    request_url = f"{server_api_url}/get_podcast_list/de/{api_secret_key}"
    response = requests.get(request_url)
    podcast_list = response.json()

    #print(podcast_list)

    podcast_list_test_dev_pool = [podcast for podcast in podcast_list if (podcast['count'] < test_dev_episodes_threshold)]

    dev_set = random.sample(podcast_list_test_dev_pool, 10)
    podcast_list_test_dev_pool = [x for x in podcast_list_test_dev_pool if x not in dev_set]

    test_set = random.sample(podcast_list_test_dev_pool, 10)

    train_set = [x for x in podcast_list if (x not in dev_set) and (x not in test_set)]

    #print(dev_set)
    #print(test_set)

    dev_podcasts = []
    for elem in dev_set:
        dev_podcasts += [process_podcast(server_api_url, api_secret_key, elem['title'], audio_dataset_location, replace_audio_dataset_location)]

    write_kaldi_dataset(dev_podcasts, 'data/dev/')    

    test_podcasts = []
    for elem in test_set:
        test_podcasts += [process_podcast(server_api_url, api_secret_key, elem['title'], audio_dataset_location, replace_audio_dataset_location)]

    write_kaldi_dataset(test_podcasts, 'data/test/')

    train_podcasts = []
    for elem in train_set:
        try:
            train_podcasts += [process_podcast(server_api_url, api_secret_key, elem['title'], audio_dataset_location, replace_audio_dataset_location)]
        except:
            print('Warning: error in ', elem['title'], 'ignoring entire podcast...')
            traceback.print_exc()
            continue
    write_kaldi_dataset(train_podcasts, 'data/train/')



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create a dataset (Kaldi format) with the server.py API')
    parser.add_argument('-d', '--dev', default=10, dest='dev_n', help='Sample dev set from n speakers/podcasts', type=int)
    parser.add_argument('-t', '--test', default=10, dest='test_n', help='Sample test set from n speakers/podcasts', type=int)
    parser.add_argument('-n', '--test_dev_episodes_threshold', default=10, dest='test_dev_episodes_threshold',
      help='Only sample the test and dev set from shorter podcasts, where len(episodes) is smaller than this value', type=int)
    parser.add_argument('--debug', dest='debug', help='Start with debugging enabled',
                        action='store_true', default=False)

    args = parser.parse_args()

    random.seed(42)
    config = load_config()
    api_secret_key = config["secret_api_key"]
    server_api_url = config["server_api_url"]
    audio_dataset_location = config["audio_dataset_location"]
    replace_audio_dataset_location = config["replace_audio_dataset_location"]
    process(server_api_url, api_secret_key, args.dev_n, args.test_n, args.test_dev_episodes_threshold, audio_dataset_location, replace_audio_dataset_location)
