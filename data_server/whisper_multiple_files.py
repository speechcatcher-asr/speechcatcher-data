import abc
import requests
import ffmpeg
from transformers import WhisperForConditionalGeneration, AutoProcessor
from transformers.utils import is_flash_attn_2_available
import numpy as np
import torch
import io
from scipy.io.wavfile import read as wav_read
import json
import copy

class WhisperMultipleFile(abc.ABC):
    '''Base class for Whisper implementations that work on multiple files.'''

    def __init__(self, model_name='large-v3', device='cuda', language='english', beam_size=5):
        self.model_name = model_name
        self.device = device
        self.language = language
        self.beam_size = beam_size
        self.model = None

    @abc.abstractmethod
    def load_model(self):
        '''Load the model for transcription.'''
        pass

    @abc.abstractmethod
    def transcribe_batch(self, audio_urls, language=None, params=None):
        '''Transcribe a batch of audio files.'''
        pass

    @abc.abstractmethod
    def write_vtt_batch(self, transcriptions, file_paths):
        '''Write VTT files for a batch of transcriptions.'''
        pass


class BatchedTransformerWhisper(WhisperMultipleFile):
    '''A speechcatcher-data abstraction for batched transcription on multiple files of similar length using Huggingface's Transformers Whisper.
       See also https://github.com/huggingface/transformers/blob/main/src/transformers/models/whisper/generation_whisper.py
    '''

    def __init__(self, model_name='large-v3', device='cuda', language='english', beam_size=5):
        super().__init__(model_name, device, language, beam_size)
        self.processor = None
        self.default_params = {
                'task': 'transcribe',
                'language': language,
                'is_multilingual': True,
                'return_timestamps': True,
                'return_segments': True,
                'num_beams': beam_size,
                'do_sample': True,
                'condition_on_prev_tokens': True,
                'temperature': 0.7,
            }


    def load_model(self, graph_compile=False):
        model_id = f"openai/whisper-{self.model_name}"
        self.processor = AutoProcessor.from_pretrained(model_id)
        attn = "flash_attention_2" if is_flash_attn_2_available() else "sdpa"
        self.model = WhisperForConditionalGeneration.from_pretrained(model_id, attn_implementation=attn)
        if graph_compile:
            self.model.generation_config.cache_implementation = "static"
            self.model.forward = torch.compile(self.model.forward, mode="reduce-overhead", fullgraph=True)
        self.model.to(self.device).half()
        print('Model loaded to GPU!')

    def convert_audio_in_memory(self, audio_url):
        """Converts an audio file to 16 kHz mono WAV using FFmpeg, directly in memory."""
        try:
            out, _ = (
                ffmpeg.input(audio_url)
                .output('pipe:', format='wav', acodec='pcm_s16le', ac=1, ar='16k')
                .run(capture_stdout=True, capture_stderr=True)
            )
            return np.frombuffer(out, np.int16).flatten().astype(np.float32) / 32768.0
        except ffmpeg.Error as e:
            print("FFmpeg error occurred:", e.stderr.decode('utf-8'))
            return None

    def get_transcript_segments(self, results, strip_segment_text=True):
        """Extracts transcript segments from the model's batch results."""
        batch_list = []
        for batch in results['segments']:
            segments_list = []
            for seg in batch:
                start = seg['start'].item() if hasattr(seg['start'], 'item') else float(seg['start'])
                end = seg['end'].item() if hasattr(seg['end'], 'item') else float(seg['end'])
                text = self.processor.tokenizer.decode(seg['tokens'], skip_special_tokens=True)
                segments_list.append({"start": start, "end": end, "text": text.strip() if strip_segment_text else text})
            batch_list.append(segments_list)
        return batch_list

    def transcribe_batch(self, audio_urls, runs=1, language=None, params=None):
        if params is None:
            params = copy.deepcopy(self.default_params)

        raw_audio_data = []
        for url in audio_urls:
            audio_data = self.convert_audio_in_memory(url)
            raw_audio_data.append(audio_data)
            #if audio_data:
            #    rate, data = wav_read(io.BytesIO(audio_data))
            #    #raw_audio_data.append(data.astype(np.float32) / 32767.0)
            #    raw_audio_data.append(data.astype(np.float32))
            #else:
            #    # Handle error in conversion by appending empty array
            #    raw_audio_data.append(np.array([])) 

        # see https://github.com/huggingface/transformers/blob/main/src/transformers/models/whisper/feature_extraction_whisper.py
        # make sure to not truncate the input audio + to return the `attention_mask` and to pad to the longest audio
        inputs = self.processor(raw_audio_data, return_tensors="pt",
                                padding="longest",
                                return_attention_mask=True,
                                do_normalize=True,
                                truncation=False,
                                sampling_rate=16000)

        multi_run_transcriptions = []

        if inputs.input_features.shape[-1] <= 3000:
            # we in-fact have short-form ASR (less than 30s) -> pre-process accordingly
            # see https://github.com/huggingface/transformers/issues/30740
            inputs = processor(raw_audio_data, return_tensors="pt", sampling_rate=16000, do_normalize=True, truncation=True)
            print('Short input detected (<=30s), using short-form pre-processor.')
        else:
            print('Long input detected (>30s), using long-form pre-processor.')

        # also convert inputs to 16 bit floats
        inputs = inputs.to(self.device, torch.float16)

        for i in range(runs):
            print('run',i)
            # Start transcription on the batch
            # see https://github.com/huggingface/transformers/blob/main/src/transformers/models/whisper/generation_whisper.py
            results = self.model.generate(**inputs, **params)

            with open('transformer_whisper_debug_output.json', 'w') as file:
                json.dump(results, file, indent=4, default=self.default_converter)

            transcriptions = self.get_transcript_segments(results)
            multi_run_transcriptions.append(transcriptions)
            if runs > 1:
                params['temperature']+=0.1

        if runs == 1:
            return transcriptions
        else:
            return multi_run_transcriptions

    def write_vtt_batch(self, transcriptions, file_paths):
        for transcription, file_path in zip(transcriptions, file_paths):
            with open(file_path, 'w') as file:
                self.write_vtt(transcription, file)

    def write_vtt(self, transcription, file):
        print("WEBVTT\n", file=file)
        for segment in transcription:
            print(
                f"{self.format_timestamp(segment['start'])} --> {self.format_timestamp(segment['end'])}\n"
                f"{segment['text'].strip().replace('-->', '->')}\n",
                file=file,
                flush=True,
            )

    def format_timestamp(self, seconds):
        """Formats a timestamp in the format HH:MM:SS.mmm."""
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        milliseconds = int((seconds - int(seconds)) * 1000)
        return f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}.{milliseconds:03}"

    def default_converter(self, o):
        if isinstance(o, np.ndarray):
            return 'np:' + str(len(o.tolist()))
        elif isinstance(o, torch.Tensor):
            rlist = o.cpu().numpy().tolist()
            if type(rlist) == float:
                return rlist
            return 't:' + str(len(rlist))
        raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

