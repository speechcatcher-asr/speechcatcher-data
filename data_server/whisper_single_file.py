import whisper
from faster_whisper import WhisperModel, BatchedInferencePipeline
import io

class WhisperSingleFile:
    def __init__(self, model_name='large-v3', device='cuda', language='english', beam_size=5):
        self.model_name = model_name
        self.device = device
        self.language = language
        self.model = None
        self.beam_size = beam_size
        self.default_params = {
            'task': 'transcribe',
            'temperature': (0.0, 0.2, 0.4, 0.6, 0.8, 1.0),
            'best_of': self.beam_size,
            'beam_size': self.beam_size,
            'suppress_tokens': "-1",
            'condition_on_previous_text': True,
            'fp16': True,
            'compression_ratio_threshold': 2.4,
            'logprob_threshold': -1.,
            'no_speech_threshold': 0.6
        }

    def load_model(self):
        raise NotImplementedError('This method should be overridden by subclasses.')

    def transcribe(self, url, params=None):
        raise NotImplementedError('This method should be overridden by subclasses.')

    def write_vtt(self, transcript, file):
        raise NotImplementedError('This method should be overridden by subclasses.')

class WhisperOriginal(WhisperSingleFile):
    def load_model(self):
        self.model = whisper.load_model(self.model_name, device=self.device)

    def transcribe(self, url, language=None, vad_filter=False, params=None):
        if params is None:
            params = self.default_params
        if language is not None:
            params.update({'language': language})
        print('Running single-file transcription with OG Whisper fp16 implementation on:', url)
        print('Beam size is:', params['beam_size'])
        return self.model.transcribe(url, **params)

    def write_vtt(self, result, file):
        print("WEBVTT\n", file=file)
        for segment in result['segments']:
            print(
                f"{whisper.utils.format_timestamp(segment['start'])} --> {whisper.utils.format_timestamp(segment['end'])}\n"
                f"{segment['text'].strip().replace('-->', '->')}\n",
                file=file,
                flush=True,
            )

class FasterWhisper(WhisperSingleFile):
    def __init__(self, model_name='large-v3', device='cuda', language='en', beam_size=5):
        super().__init__(model_name, device, language, beam_size)
        del self.default_params['fp16']
        del self.default_params['logprob_threshold']
        del self.default_params['suppress_tokens']
        self.default_params['log_progress'] = True
        self.default_params['without_timestamps'] = False
        self.batched_model = None

    def load_model(self):
        self.model = WhisperModel(self.model_name, device=self.device, compute_type='float16')
        self.batched_model = BatchedInferencePipeline(model=self.model)

    def transcribe(self, url, language=None, vad_filter=False, params=None):
        if params is None:
            params = self.default_params
        params.update({'vad_filter': True})
        if language is not None:
            params['language'] = language
        print('Running single-file transcription with CTranslate2 FasterWhisper fp16 implementation on:', url)
        print('Beam size is:', params['beam_size'])
        result = self.batched_model.transcribe(url, **params)
        segments, info = result
        return {'segments': list(segments), 'language': info.language}

    def write_vtt(self, result, file):
        print("WEBVTT\n", file=file)
        for segment in result['segments']:
            print(
                f"{whisper.utils.format_timestamp(segment.start)} --> {whisper.utils.format_timestamp(segment.end)}\n"
                f"{segment.text.strip().replace('-->', '->')}\n",
                file=file,
                flush=True,
            )

