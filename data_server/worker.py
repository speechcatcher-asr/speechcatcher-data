import requests
import traceback
import sys
import argparse
import torch
import numpy as np
import io
import time
from utils import load_config
from whisper.utils import format_timestamp
from typing import Iterator, TextIO

from whisper_single_file import WhisperOriginal, FasterWhisper, WhisperX, WhisperCpp
from whisper_multiple_files import BatchedTransformerWhisper

podcast_initial_prompts = {
    'en': 'Podcast author: {}, podcast title: {}',
    'de': 'Podcast-Autor: {}, Podcast-Titel: {}',
    'fr': 'Auteur du podcast : {}, Titre du podcast : {}',
    'pl': 'Autor podcastu: {}, Tytuł podcastu: {}',
    'es': 'Autor del podcast: {}, Título del podcast: {}',
    'it': 'Autore del podcast: {}, Titolo del podcast: {}',
    'nl': 'Podcast auteur: {}, Podcast titel: {}',
    'sv': 'Podcastens författare: {}, Podcastens titel: {}',
    'da': 'Podcast forfatter: {}, Podcast titel: {}',
    'fi': 'Podcastin tekijä: {}, Podcastin otsikko: {}',
    'no': 'Podkast forfatter: {}, Podkast tittel: {}',
    'pt': 'Autor do podcast: {}, Título do podcast: {}',
    'ru': 'Автор подкаста: {}, Название подкаста: {}',
    'cs': 'Autor podcastu: {}, Název podcastu: {}',
    'hu': 'Podcast szerző: {}, Podcast címe: {}',
    'ro': 'Autor podcast: {}, Titlu podcast: {}',
    'bg': 'Автор на подкаст: {}, Заглавие на подкаст: {}',
    'el': 'Συγγραφέας podcast: {}, Τίτλος podcast: {}',
    'tr': 'Podcast yazarı: {}, Podcast başlığı: {}'
}

def cancel_work(server, secret_api_key, wid, api_version='apiv1'):
    """ Cancels the work in progress for one task on the server. """
    print(f'Trying to cancel {wid}...')
    cancel_work = f'{server}/{api_version}/cancel_work/{wid}/{secret_api_key}'
    resp = requests.get(url=cancel_work)
    data = resp.json()
    assert(data['success'] == True)

def cancel_work_batch(server, api_secret_key, wids, api_version='apiv1'):
    """ Cancels the work in progress for a batch of tasks on the server. """
    print(f'Trying to cancel {wids}...')
    cancel_url = f'{server}/{api_version}/cancel_work_batch/{api_secret_key}'
    resp = requests.post(cancel_url, json={'wids': wids})
    data = resp.json()
    print('Cancelled work in progress:', data)
    return data

def transcribe_loop(server, language, secret_api_key, model_name='small', api_version='apiv1', implementation='original', beam_size=5, use_local_url=False):
    print(f'Loading whisper model {model} with {implementation} implementation')

    # Initialize the selected transcription implementation
    # Abstraction classes for major whisper implementations can be found in whisper_single_file.py
    # Note: original implementation is still recommended for long-form transcription for the time being,
    # as all the other faster implementation seem to struggle a lot more with
    # hallucinations.

    if implementation == 'original':
        transcriber = WhisperOriginal(beam_size=beam_size, model_name=model_name)
    elif implementation == 'faster':
        transcriber = FasterWhisper(beam_size=beam_size, model_name=model_name)
    elif implementation == 'X':
        transcriber = WhisperX(beam_size=beam_size, model_name=model_name)
    elif implementation == 'cpp':
        transcriber = WhisperCpp(beam_size=beam_size, model_name=model_name)
    else:
        raise NotImplementedError("Not implemented:", implementation)

    transcriber.load_model()
    get_work_url = f'{server}/{api_version}/get_work/{language}/{secret_api_key}'
    print(f'{get_work_url=}')

    while True:
        wip = False
        try:
            # Step 1) Get a url to transcribe from the transcription server
            resp = requests.get(url=get_work_url)
            print('server response:', resp)
            data = resp.json()
            assert(data['transcript_file'] == '')
            assert(data['cache_audio_url'] != '')
            assert(data['success'] == True)

            title = data.get('episode_title') or None
            author = data.get('authors') or None

            url = data['cache_audio_url']
            if use_local_url:
                assert(data['local_cache_audio_url'] != '')
                url = data['local_cache_audio_url']

            wid = data['wid']

            print('New job:', data)
            print('Work ID:', wid)

            # Step 2) Confirm we are taking the job
            confirm_work_url = f'{server}/{api_version}/register_wip/{wid}/{secret_api_key}'
            print(f'{confirm_work_url=}')
            resp = requests.get(url=confirm_work_url)
            data = resp.json()
            print('Confirmed:', data)
            assert(data['success'] == True)
            wip = True

            # Generate the prompt based on the language, defaulting to English if the language code is not found
            prompt = podcast_initial_prompts.get(language, podcast_initial_prompts['en']).format(author, title) if author or title else ''

            if prompt[-1] == '\n':
                prompt = prompt[:-1]

            if prompt and prompt[-1] not in '.!?':
                prompt += '.'
            prompt += '\n'

            # Step 3) Use whisper to transcribe and obtain a vtt.
            # Provide author and title as additional information (prompt).
            print('Transcribing with prompt:', prompt)
            result = transcriber.transcribe(url, language=language, duration=-1, initial_prompt = prompt)
            print('Done!')

            print('Model reported language:', result['language'])
            assert(result['language'] == language)

            fi = io.StringIO('')
            transcriber.write_vtt(result, file=fi)
            fi.seek(0)

            # Step 4) Upload vtt and close the memory StringIO file
            files = {'file': fi}
            data = {'model': f'{implementation}_bs{beam_size}'}
            upload_url = f'{server}/{api_version}/upload_result/{wid}/{secret_api_key}'
            print(f"{upload_url=}")

            resp = requests.post(upload_url, files=files, data=data)
            data = resp.json()
            assert(data['success'] == True)

            # Cleanup, just making sure data doesnt get mixed up in the next iteration
            wip = False
            vtt_str = fi.read()
            fi.close()
            del fi
            del result

            print('Done uploading new VTT file!')

        except KeyboardInterrupt:
            print("Keyboard interrupt")
            if wip:
                print('Canceled with work in progress:', wid)
                cancel_work(server, secret_api_key, wid, api_version)
            sys.exit(-10)

        except Exception as e:
            print("Exception encountered in transcribe_loop:", e)
            traceback.print_exc()
            if wip:
                print('Canceled with work in progress:', wid)
                cancel_work(server, secret_api_key, wid, api_version)
            time.sleep(30)

def upload_results_batch(server, api_version, secret_api_key, wids, results):
    """Uploads transcription results for a batch of work items."""
    upload_url = f"{server}/{api_version}/upload_result_batch/{secret_api_key}"
    files = []
    data = []

    # Preparing the multipart/form-data with files and data
    for wid, result in zip(wids, results):
        # Creating a virtual file object containing the result (assuming result is VTT text)
        file_object = io.StringIO(result)
        # Each file needs a unique key in the 'files' dict for the multipart upload.
        # ('file', (filename, fileobject, content_type))
        files.append(('file', (f"{wid}.vtt", file_object.getvalue(), 'text/vtt')))
        # Including the model name or any additional data as part of the form data
        data.append(('model', f'whisper_{wid}'))

    # POST request with files and data
    response = requests.post(upload_url, files=files, data=data)
    for _, file_tuple in files:
        file_tuple[1].close()

    return response.json()

def register_wip_batch(server, api_version, secret_api_key, wids):
    """
    Registers a batch of work items as in progress by sending a POST request to the server.
    
    :param server: URL of the server where the API is hosted.
    :param api_version: API version to access the correct endpoint.
    :param secret_api_key: Secret key for API access.
    :param wids: List of work item IDs (wids) that are to be registered.
    :return: JSON response from the server indicating success or failure.
    """

    url = f"{server}/{api_version}/register_wip_batch/{secret_api_key}"
    payload = {'wids': wids} # Payload containing the list of work IDs
    response = requests.post(url, json=payload)

    if response.status_code == 200:
        return response.json()
    else:
        # In case of a non-200 response, log and return an error message
        print(f"Failed to register work in progress. Status Code: {response.status_code}, Response: {response.text}")
        return {'success': False, 'error': 'Failed to register work in progress with the server.'}

def transcribe_loop_batch(server, language, secret_api_key, model='small', api_version='apiv1', batch_size=5, beam_size=5):
    print(f"Loading Whisper model {model} with batched_transformer implementation")

    transcriber = BatchedTransformerWhisper(beam_size=beam_size)
    transcriber.load_model()
    get_work_url = f'{server}/{api_version}/get_work_batch/{language}/{secret_api_key}/{batch_size}'
    print(f'URL for getting work: {get_work_url}')

    while True:
        wip = False
        try:
            # Step 1: Get a batch of work to transcribe
            resp = requests.get(url=get_work_url)
            work_batch = resp.json()

            if not work_batch['success']:
                print("Failed to fetch work batch:", work_batch)
                continue

            urls = [task['episode_audio_url'] for task in work_batch['tasks']]
            wids = [task['wid'] for task in work_batch['tasks']]
            wip = True

            print('Fetched new batch of jobs:', wids)

            # Step 2: Register work in progress for the fetched batch
            register_response = register_wip_batch(server, api_version, secret_api_key, wids)
            if not register_response['success']:
                print("Failed to register work in progress:", register_response)
                continue

            print('Batch registered:', register_response)

            # Step 3: Transcribe batch
            results = transcriber.transcribe_batch(urls, language=language)
            vtt_results = []
            for result in results:
                fi = io.StringIO('')
                transcriber.write_vtt(result['segments'], file=fi)
                fi.seek(0)
                vtt_results.append(fi.getvalue())
                fi.close()

            # Step 4: Upload results
            upload_results_batch(server, api_version, secret_api_key, wids, vtt_results)
            wip = False

        except KeyboardInterrupt:
            print("Keyboard interrupt")
            if wip:
                print('Canceled with work in progress:', wids)
                cancel_work_batch(server, secret_api_key, wids, api_version)
            sys.exit(-10)

        except Exception as e:
            print("Exception encountered in transcribe_loop_batch:", e)
            traceback.print_exc()
            if wip:
                print('Canceled with work in progress:', wids)
                cancel_work_batch(server, secret_api_key, wids, api_version)
            time.sleep(30)

if __name__ == '__main__':
    config = load_config()
    default_lang = 'en'
    default_beam_size = 5
    default_whisper_model = 'large_v3'

    if 'podcast_language' in config:
        default_lang = config['podcast_language']

    if 'whisper_model' in config:
        default_whisper_model = config['whisper_model']

    server_api_url = config.get('server_api_url', "http://mini1.local:5562/apiv1/")
    server_url, api_version = server_api_url.rstrip('/').rsplit('/', 1)

    parser = argparse.ArgumentParser(description='Worker that uses whisper to transcribe')
    parser.add_argument('-s', '--server-address', default=server_url, dest='server', help=f'Server address to connect to. Default: {server_url}')
    parser.add_argument('-l', '--language', default=default_lang, dest='language', help=f'Language (used in the queries to the server). Default: {default_lang}')
    parser.add_argument('--debug', dest='debug', help='Start with debugging enabled', action='store_true', default=False)
    parser.add_argument('--implementation', choices=['original', 'faster', 'X', 'batched_transformer', 'cpp'], default='original', help='Select the whisper implementation to use. Default: original')
    parser.add_argument('--beam-size', type=int, default=default_beam_size, help=f'Decoding beam size. Default: {default_beam_size}')
    parser.add_argument('--model-name', type=str, default=default_whisper_model, help=f'Whisper model name tag. Default: {default_whisper_model}')
    parser.add_argument('--api-version', default=api_version, help=f'API version to use. Default: {api_version}')
    parser.add_argument('--use_local_url', dest='use_local_url', help='Use local LAN URL instead of global internet URL.', action='store_true', default=False)
    args = parser.parse_args()

    if args.implementation == 'batched_transformer':
        transcribe_loop_batch(args.server, args.language, config['secret_api_key'], model=args.model_name, api_version=args.api_version, beam_size=args.beam_size)
    else:
        transcribe_loop(args.server, args.language, config['secret_api_key'], model=args.model_name, implementation=args.implementation, api_version=args.api_version, beam_size=args.beam_size, use_local_url=args.use_local_url)
