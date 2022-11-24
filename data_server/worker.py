import requests
import traceback
import sys
import argparse
import whisper
import io
import time
from whisper.utils import write_vtt
from utils import load_config

def cancel_work(server, secret_api_key, wid, api_version='apiv1'):
    print(f'Trying to cancel {wid}...')
    cancel_work = f'{server}/{api_version}/cancel_work/{wid}/{secret_api_key}'

    resp = requests.get(url=cancel_work)
    data = resp.json()
    assert(data['success'] == True)

    return

def transcribe_loop(server, language, secret_api_key, model='small', api_version='apiv1'):
   
    print(f'Loading whisper model {model}')
    model = whisper.load_model(model)
    wip = False
    print('Done')

    get_work_url = f'{server}/{api_version}/get_work/{language}/{secret_api_key}'
    print(f'{get_work_url=}')
    while True:
        try:
            # Step 1) Get a url to transcribe from the transcription server

            resp = requests.get(url=get_work_url)
            data = resp.json()

            assert(data['transcript_file'] == '')
            assert(data['cache_audio_url'] != '')
            assert(data['success'] == True)

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

            # Step 3) Use whisper to transcribe and obtain a vtt
            print("Transcribing with whisper...")
            result = model.transcribe(url, language=language)
            print('Done!')

            print('model reported language:', result["language"])
            assert(result["language"] == language)

            fi = io.StringIO('')
            write_vtt(result["segments"], file=fi)

            fi.seek(0)

            # Step 4) Upload vtt and close the memory StringIO file
            files = {'file': fi}
            upload_url = f'{server}/{api_version}/upload_result/{wid}/{secret_api_key}'
            print(f"{upload_url=}")

            resp = requests.post(upload_url, files=files)
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
    parser = argparse.ArgumentParser(description='Worker that uses whisper to transcribe')
    parser.add_argument('-s', '--server-address', default='https://speechcatcher.net/', dest='server', help='Server address to connect to.')
    parser.add_argument('-l', '--language', default='de', dest='language', help='Language (used in the queries to the server).')
    parser.add_argument('--debug', dest='debug', help='Start with debugging enabled',
                                                    action='store_true', default=False)

    args = parser.parse_args()

    config = load_config()

    transcribe_loop(args.server, args.language, config['secret_api_key'], model = config['whisper_model']) 
