import requests
import whisper
import ffmpeg
from transformers import WhisperForConditionalGeneration, AutoProcessor
from datasets import Dataset, load_dataset, Audio
import numpy as np
import torch
import io
from scipy.io.wavfile import read as wav_read
from utils import load_config
import inspect
from worker import write_vtt

config = load_config()

# Configuration
api_base_url = config['server_api_url'] # Base URL of the API
api_access_key = config['secret_api_key'] # API secret key

def fetch_batch(language, n, min_duration):
    """Fetch a batch of work from the server."""
    url = f"{api_base_url}/get_work_batch/{language}/{api_access_key}/{n}?min_duration={min_duration}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['tasks']
    else:
        print("Failed to fetch batch:", response.text)
        return []


def convert_audio_in_memory(audio_url):
    """Converts an audio file to 16 kHz mono WAV using FFmpeg, directly in memory."""
    try:
        out, _ = (
            ffmpeg.input(audio_url)
            .output('pipe:', format='wav', acodec='pcm_s16le', ac=1, ar='16k')
            .run(capture_stdout=True, capture_stderr=True)
        )
        return out
    except ffmpeg.Error as e:
        print("FFmpeg error occurred:")
        print(e.stderr.decode('utf-8'))  # Decode and print stderr for detailed ffmpeg error
        return None

def get_transcript_segments(results, processor, strip_segment_text=True):
    """
    Extracts transcript segments from the model's batch results and decodes them into
    a list of a list of dictionaries with 'start', 'end', and 'text' keys.
    """

    batch_list = []
    for batch in results['segments']:
        segments_list = []
        for seg in batch:
            # Convert start and end from tensor to float
            start = seg['start'].item() if hasattr(seg['start'], 'item') else float(seg['start'])
            end = seg['end'].item() if hasattr(seg['end'], 'item') else float(seg['end'])
            # Decode the token IDs stored in 'result'
            text = processor.tokenizer.decode(seg['tokens'], skip_special_tokens=True)
            segments_list.append({"start": start, "end": end, "text": text.strip() if strip_segment_text else text})
        batch_list.append(segments_list)
    return batch_list

def transcribe_batch(audio_urls, device='cuda'):
    """Uses Whisper to transcribe a batch of audio URLs."""
    model_id = "openai/whisper-large-v3"
    processor = AutoProcessor.from_pretrained(model_id)
    model = WhisperForConditionalGeneration.from_pretrained(model_id)
    model.to(device).half()

    raw_audio_data = []
    for url in audio_urls:
        audio_data = convert_audio_in_memory(url)
        print('audio_data type:', type(audio_data))
        if audio_data:
            rate, data = wav_read(io.BytesIO(audio_data))
            raw_audio_data.append(data.astype(np.float32))
        else:
            raw_audio_data.append(np.array([]))  # Handle error in conversion by appending empty array

    # assume long form
    inputs = processor(raw_audio_data, return_tensors="pt",
                       padding="longest",
                       return_attention_mask=True,
                       sampling_rate=16000)
    
    if inputs.input_features.shape[-1] < 3000:
        # we in-fact have short-form ASR (less than 30s) -> pre-process accordingly
        # see https://github.com/huggingface/transformers/issues/30740
        inputs = processor(raw_audio_data, return_tensors="pt", sampling_rate=16000)
        print('Short input detected (<30s), using short-form pre-processor.')

    # also convert inputs to 16 bit floats
    inputs = inputs.to(device, torch.float16)

    # Start transcription on the batch
    results = model.generate(**inputs, condition_on_prev_tokens=True,
                             task="transcribe",
                             return_timestamps=True,
                             #return_token_timestamps=True,
                             #output_scores=True,
                             return_segments=True)

    transcriptions = get_transcript_segments(results, processor)

    return transcriptions

if __name__ == "__main__":
    language = 'en'
    batch_size = 4
    min_duration = 280.0
    tasks = fetch_batch(language, batch_size, min_duration)
    print(tasks)
    if tasks:
        audio_urls = [task['local_cache_audio_url'] for task in tasks]
        print('audio_urls:', audio_urls)

        # transcribe a batch of input (audio) urls
        transcriptions = transcribe_batch(audio_urls)

        # write out transcriptions as vtt
        for audio_url, transcription in zip(audio_urls, transcriptions):
            print('Write transcription for:', audio_url)
            filename_out = audio_url.split('/')[-1] + '.vtt'
            with open(filename_out, 'w') as file_out:
                write_vtt(transcription, file_out)
                print(f'Wrote vtt to: {filename_out}')

    else:
        print("No tasks fetched, nothing to transcribe.")

