import requests
import traceback
import sys
import argparse
import torch
import numpy as np
import whisper
from faster_whisper import WhisperModel, BatchedInferencePipeline
import io
import time
from utils import load_config

from whisper.utils import format_timestamp

from typing import Iterator, TextIO

# The write_vtt function was replaced in whisper, its a bit annoying
# this is the old version, copied from a previous version of whisper
# see https://github.com/openai/whisper/commit/da600abd2b296a5450770b872c3765d0a5a5c769
def write_vtt(transcript: Iterator[dict], file: TextIO, fast_whisper=False):
    print("WEBVTT\n", file=file)
    for segment in transcript:
        if fast_whisper:
            # make faster-whisper output compatible with OG whisper
            segment = {"start": segment.start, "end": segment.end, "text": segment.text}
        print(segment)
        print(
            f"{format_timestamp(segment['start'])} --> {format_timestamp(segment['end'])}\n"
            f"{segment['text'].strip().replace('-->', '->')}\n",
            file=file,
            flush=True,
        )

def cancel_work(server, secret_api_key, wid, api_version='apiv1'):
    """ Cancels the work in progress for one task on the server. """
    print(f'Trying to cancel {wid}...')
    cancel_work = f'{server}/{api_version}/cancel_work/{wid}/{secret_api_key}'

    resp = requests.get(url=cancel_work)
    data = resp.json()
    assert(data['success'] == True)

    return

def cancel_work_batch(server, api_secret_key, wids, api_version='apiv1'):
    """ Cancels the work in progress for a batch of tasks on the server. """
    print(f'Trying to cancel {wid}...')
    cancel_url = f'{server}/{api_version}/cancel_work_batch/{api_secret_key}'
    
    resp = requests.post(cancel_url, json={'wids': wids})
    data = resp.json()
    print('Cancelled work in progress:', data)

    return data

def transcribe_loop(server, language, secret_api_key, model='small', api_version='apiv1', fast_whisper=True):
    print(f'Loading whisper model {model}')

    if fast_whisper:
        model = WhisperModel(model, device="cuda", compute_type="float16")
        batched_model = BatchedInferencePipeline(model=model)
    else:
        model = whisper.load_model(model)
    print('Done')

    get_work_url = f'{server}/{api_version}/get_work/{language}/{secret_api_key}'
    print(f'{get_work_url=}')
    while True:
        wip = False
        try:
            # Step 1) Get a url to transcribe from the transcription server

            resp = requests.get(url=get_work_url)
            data = resp.json()

            assert(data['transcript_file'] == '')
            assert(data['cache_audio_url'] != '')
            assert(data['success'] == True)

            # Title is later used as inital prompt.
            # It has to be None, if there is no title.
            title = None
            if 'episode_title' in data:
                title = data['episode_title']
                if title == '':
                    title = None

            author = None
            if 'authors' in data:
                author = data['authors']
                if author == '':
                    author = None

            url = data['cache_audio_url']
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

            # Step 3) Use whisper (faster-whisper) to transcribe and obtain a vtt.
            # Provide author and title as additional information (prompt).
 
            prompt = f'Author: {author}, Title: {title}'

            if prompt[-1] == '\n':
                prompt = prompt[:-1]

            if not (prompt[-1] == '.' or prompt[-1] == '!' or prompt[-1] == '?'):
                prompt += '.'

            prompt += '\n'

            model_beam_size = 3
            bs = model_beam_size

            if fast_whisper:
                print('Transcribing with fast whisper...')
                print('Prompt:', prompt)
                # If vad filter=False, we get: No clip timestamps found. Set 'vad_filter' to True or provide 'clip_timestamps'.
                # Looks like condition on previous text is also ignored
                segments, info = batched_model.transcribe(url, vad_filter=True, language=language, task='transcribe', temperature=(0.0, 0.2, 0.4, 0.6, 0.8, 1.0), best_of=bs, beam_size=bs, condition_on_previous_text=True, initial_prompt=prompt, batch_size=8)
                # OG whisper compatibility
                result = {'segments': list(segments), 'language': info.language}
            else:
                # There might be a bug in whisper where the default of the the command line process doesn't match the defaults of the transcribe function, the parameters below replicate the command line defaults
                print('Transcribing with whisper...')
                result = model.transcribe(url, language=language, task='transcribe', temperature=(0.0, 0.2, 0.4, 0.6, 0.8, 1.0), best_of=bs, beam_size=bs, suppress_tokens="-1", condition_on_previous_text=True, fp16=True, compression_ratio_threshold=2.4, logprob_threshold=-1., no_speech_threshold=0.6)
           
            print('Done!')

            print('model reported language:', result['language'])
            assert(result['language'] == language)

            fi = io.StringIO('')
            write_vtt(result['segments'], file=fi, fast_whisper=fast_whisper)

            fi.seek(0)

            # Step 4) Upload vtt and close the memory StringIO file
            files = {'file': fi}

            # Add the model name as part of the request payload
            data = {'model': f'fwhisper_fp16_bs{bs}' if fast_whisper else f'whisper_fp16_bs{bs}'}

            upload_url = f'{server}/{api_version}/upload_result/{wid}/{secret_api_key}'
            print(f"{upload_url=}")

            # Include 'data' with the POST request
            resp = requests.post(upload_url, files=files, data=data)

            # Parse and check the response
            data = resp.json()
            assert(data['success'] == True)

            wip = False
            vtt_str = fi.read()
            fi.close()

            # Cleanup, just making sure data doesnt get mixed up in the next iteration
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

    return

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
    # Closing all StringIO objects
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
    payload = {'wids': wids}  # Payload containing the list of work IDs
    response = requests.post(url, json=payload)  # Sending a POST request with the payload as JSON

    if response.status_code == 200:
        return response.json()  # Return the JSON response if request was successful
    else:
        # In case of a non-200 response, log and return an error message
        print(f"Failed to register work in progress. Status Code: {response.status_code}, Response: {response.text}")
        return {'success': False, 'error': 'Failed to register work in progress with the server.'}


def transcribe_loop_batch(server, language, secret_api_key, model='small', api_version='apiv1', batch_size=5):
    from transformers import AutoProcessor, WhisperForConditionalGeneration
    from datasets import load_dataset, Audio

    print(f"Loading Whisper model {model}")
    processor = AutoProcessor.from_pretrained(f"openai/whisper-{model}.en")
    whisper_model = WhisperForConditionalGeneration.from_pretrained(f"openai/whisper-{model}.en", torch_dtype=torch.float16)
    whisper_model.to("cuda")

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
            register_response = register_wip_batch(wids)
            if not register_response['success']:
                print("Failed to register work in progress:", register_response)
                continue

            print('Batch registered:', register_response)

            # Transcription
            ds = load_dataset("text", data_files=urls)["train"]
            ds = ds.cast_column("audio", Audio(sampling_rate=16000))

            raw_audio = [x["array"].astype(np.float32) for x in ds["audio"]]
            inputs = processor(raw_audio, return_tensors="pt", padding="longest", return_attention_mask=True, sampling_rate=16000)
            inputs = inputs.to("cuda", torch.float16)

            # Transcribe
            results = whisper_model.generate(**inputs, condition_on_prev_tokens=True, temperature=(0.0, 0.2, 0.4, 0.6, 0.8, 1.0), return_timestamps=True)
            decoded_results = processor.batch_decode(results, skip_special_tokens=True)

            # Step 4: Upload results
            upload_results_batch(server, api_version, secret_api_key, wids, decoded_results)
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
    default_lang = 'de'

    if 'podcast_language' in config:
        default_lang = config['podcast_language']

    parser = argparse.ArgumentParser(description='Worker that uses whisper to transcribe')
    parser.add_argument('-s', '--server-address', default='https://speechcatcher.net/', dest='server', help='Server address to connect to.')
    parser.add_argument('-l', '--language', default='de', dest='language', help='Language (used in the queries to the server).')
    parser.add_argument('--debug', dest='debug', help='Start with debugging enabled',
                                                    action='store_true', default=False)
    args = parser.parse_args()

    transcribe_loop(args.server, args.language, config['secret_api_key'], model = config['whisper_model']) 
