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


def convert_audio_in_memory_old(audio_url):
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

def convert_audio_in_memory(audio_url):
    """Converts an audio file to 16 kHz mono WAV using FFmpeg, directly in memory, and pads if necessary."""
    try:
        # Convert and capture output in-memory
        out, _ = (
            ffmpeg.input(audio_url)
            .output('pipe:', format='wav', acodec='pcm_s16le', ac=1, ar='16k')
            .run(capture_stdout=True, capture_stderr=True)
        )
        # Load the audio data from bytes
        wav_file = io.BytesIO(out)
        rate, data = wav_read(wav_file)
        
        # Calculate the minimum length (30 seconds at 16 kHz)
        min_length = 30 * rate  # 30 seconds * samples per second
        
        if len(data) < min_length:
            # If the data is too short, pad it with zeros
            padding = np.zeros(min_length - len(data), dtype=np.int16)
            data = np.concatenate((data, padding))
        
        # Ensure the data is returned as bytes
        return io.BytesIO(bytes(data))
    except ffmpeg.Error as e:
        print("FFmpeg error occurred:")
        print(e.stderr.decode('utf-8'))
        return None

def transcribe_batch(audio_urls, device='cuda'):
    """Uses Whisper to transcribe a batch of audio URLs."""
    model_id = "openai/whisper-large-v3"
    processor = AutoProcessor.from_pretrained(model_id)
    model = WhisperForConditionalGeneration.from_pretrained(model_id)
    model.to(device).half()

    raw_audio_data = []
    for url in audio_urls:
        audio_data = convert_audio_in_memory_old(url)
        print('audio_data type:', type(audio_data))
        if audio_data:
            rate, data = wav_read(io.BytesIO(audio_data))
            raw_audio_data.append(data.astype(np.float32))
        else:
            raw_audio_data.append(np.array([]))  # Handle error in conversion by appending empty array

    # assume long form
    inputs = processor(raw_audio_data, return_tensors="pt", padding="longest", return_attention_mask=True, sampling_rate=16000)
    
    if inputs.input_features.shape[-1] < 3000:
        # we in-fact have short-form ASR (less than 30s) -> pre-process accordingly
        # see https://github.com/huggingface/transformers/issues/30740
        inputs = processor(raw_audio_data, return_tensors="pt", sampling_rate=16000)
        print('Short input detected (<30s), using short-form pre-processor.')

    # also convert inputs to 16 bit floats
    inputs = inputs.to(device, torch.float16)

    # Start transcription on the batch
    results = model.generate(**inputs, condition_on_prev_tokens=True)
    transcriptions = processor.batch_decode(results, skip_special_tokens=True)

    return transcriptions

if __name__ == "__main__":
    language = 'en'
    batch_size = 4
    min_duration = 280.0
    tasks = fetch_batch(language, batch_size, min_duration)
    print(tasks)
    if tasks:
        audio_urls = [task['cache_audio_url'] for task in tasks]
        print('audio_urls:', audio_urls)

        transcriptions = transcribe_batch(audio_urls)
        
        print('transcriptions:',transcriptions)

        #write_vtt_files(transcriptions, tasks)
    else:
        print("No tasks fetched, nothing to transcribe.")

