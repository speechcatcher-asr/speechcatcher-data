import requests
import whisper
from transformers import WhisperForConditionalGeneration, AutoProcessor
from datasets import load_dataset, Audio
import numpy as np
import torch
import io

# Configuration
api_base_url = "http://192.168.0.5:4280/apiv1/"  # Base URL of the API
api_access_key = "password4269"  # API secret key

def fetch_batch(language, n):
    """Fetch a batch of work from the server."""
    url = f"{api_base_url}/get_work_batch/{language}/{api_access_key}/{n}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['tasks']
    else:
        print("Failed to fetch batch:", response.text)
        return []

def transcribe_batch(audio_urls):
    """Uses Whisper to transcribe a batch of audio URLs."""
    model_id = "openai/whisper-large-v3"
    processor = AutoProcessor.from_pretrained(model_id)
    model = WhisperForConditionalGeneration.from_pretrained(model_id)
    model.to("cuda")

    # Prepare dataset
    ds = load_dataset("text", data_files={"train": audio_urls}, split="train")
    ds = ds.cast_column("audio", Audio(sampling_rate=16000))
    raw_audio = [x["array"].astype(np.float32) for x in ds["audio"]]

    inputs = processor(raw_audio, return_tensors="pt", padding="longest", return_attention_mask=True, sampling_rate=16000)
    inputs = inputs.to("cuda", torch.float16)

    # Transcription
    results = model.generate(**inputs, condition_on_prev_tokens=True)
    transcriptions = processor.batch_decode(results, skip_special_tokens=True)

    return transcriptions

if __name__ == "__main__":
    language = 'en'
    batch_size = 4
    tasks = fetch_batch(language, batch_size)
    print(tasks)
    if tasks:
        audio_urls = [task['episode_audio_url_cache'] for task in tasks]
        print('audio_urls:', audio_urls)

        transcriptions = transcribe_batch(audio_urls)
        
        print('transcriptions:',transcriptions)

        #write_vtt_files(transcriptions, tasks)
    else:
        print("No tasks fetched, nothing to transcribe.")

