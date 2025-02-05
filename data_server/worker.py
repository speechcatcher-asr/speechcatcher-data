import requests
import traceback
import sys
import argparse
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
def write_vtt(transcript: Iterator[dict], file: TextIO, fast_whisper=True):
    print("WEBVTT\n", file=file)
    for segment in transcript:
        if fast_whisper:
            # make faster-whisper output compatible with OG whisper
            segment = {"start": segment.start, "end": segment.end, "text": segment.text}
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
