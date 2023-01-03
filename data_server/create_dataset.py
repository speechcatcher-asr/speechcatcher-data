# create a dataset (Kaldi format) with the server.py API

import requests
import random
import argparse
from utils import *

def process_podcast(server_api_url, api_secret_key,title):

    request_url = f"{server_api_url}/get_episode_list/{api_secret_key}"
    data = {'podcast_title': title}

    print('server_api_url:', request_url)

    response = requests.post(request_url, data=data)

    episode_list = response.json()

    print(episode_list)

def process(server_api_url, api_secret_key, test_dev_episodes_threshold=10):
    
    request_url = f"{server_api_url}/get_podcast_list/de/{api_secret_key}"
    response = requests.get(request_url)
    podcast_list = response.json()

    #print(podcast_list)

    podcast_list_test_dev_pool = [podcast for podcast in podcast_list if (podcast['count'] < test_dev_episodes_threshold)]

    dev_set = random.sample(podcast_list_test_dev_pool, 10)
    podcast_list_test_dev_pool = [x for x in podcast_list_test_dev_pool if x not in dev_set]

    test_set = random.sample(podcast_list_test_dev_pool, 10)

    train_set = [x for x in podcast_list if (x not in dev_set) and (x not in test_set)]

    print(dev_set)
    print(test_set)

    for elem in dev_set:
        process_podcast(server_api_url, api_secret_key,elem['title'])

    for elem in test_set:
        process_podcast(server_api_url, api_secret_key,elem['title'])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create a dataset (Kaldi format) with the server.py API')
    parser.add_argument('-d', '--dev', default=10, dest='host', help='Sample dev set from n speakers/podcasts', type=int)
    parser.add_argument('-t', '--test', default=10, dest='port', help='Sample test set from n speakers/podcasts', type=int)
    parser.add_argument('--debug', dest='debug', help='Start with debugging enabled',
                        action='store_true', default=False)

    args = parser.parse_args()

    random.seed(42)
    config = load_config()
    api_secret_key = config["secret_api_key"]
    server_api_url = 'https://speechcatcher.net/apiv1/'
    process(server_api_url, api_secret_key)
