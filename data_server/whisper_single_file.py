import whisper
import whisperx
from faster_whisper import WhisperModel, BatchedInferencePipeline
import io
from pywhispercpp.model import Model

class WhisperSingleFile:
    '''Base class for Whisper implementations that work operate on single files.'''
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

    def transcribe(self, url, language=None, duration=-1, params=None):
        raise NotImplementedError('This method should be overridden by subclasses.')

    def write_vtt(self, transcript, file):
        raise NotImplementedError('This method should be overridden by subclasses.')

class WhisperOriginal(WhisperSingleFile):
    '''A speechcatcher-data abstraction for https://github.com/openai/whisper'''
    def load_model(self):
        self.model = whisper.load_model(self.model_name, device=self.device)

    def transcribe(self, url, language=None, duration=-1, params=None):
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
    '''A speechcatcher-data abstraction for https://github.com/SYSTRAN/faster-whisper'''
    def __init__(self, model_name='large-v3', device='cuda', language='en', beam_size=5, use_vad=True):
        super().__init__(model_name, device, language, beam_size)
        # Remove arguments that are unsupported by this implemenation
        del self.default_params['fp16']
        del self.default_params['logprob_threshold']
        del self.default_params['suppress_tokens']
        # Add arguments that are specific to this implemenation
        self.default_params['log_progress'] = True
        self.default_params['without_timestamps'] = False
        self.default_params['temperature'] = 0.1
        self.default_params['vad_filter'] = use_vad
        self.batched_model = None

    def load_model(self):
        self.model = WhisperModel(self.model_name, device=self.device, compute_type='float16')
        self.batched_model = BatchedInferencePipeline(model=self.model)

    def transcribe(self, url, language=None, duration=-1, params=None):
        if params is None:
            params = self.default_params
        if language is not None:
            params['language'] = language
        print('Running single-file transcription with CTranslate2 FasterWhisper fp16 implementation on:', url)
        print('Beam size is:', params['beam_size'], ', VAD Filter:', params['vad_filter'])

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

class WhisperX(FasterWhisper):
    '''A speechcatcher-data abstraction for https://github.com/m-bain/whisperX'''
    def __init__(self, model_name='large-v3', device='cuda', language='en', beam_size=5, use_vad=True):
        super().__init__(model_name, device, language, beam_size, use_vad)
        # Remove arguments that are unsupported by this implemenation
        del self.default_params['temperature']
        del self.default_params['best_of']
        del self.default_params['beam_size']
        del self.default_params['condition_on_previous_text']
        del self.default_params['compression_ratio_threshold']
        del self.default_params['no_speech_threshold']
        del self.default_params['log_progress']
        del self.default_params['without_timestamps']
        del self.default_params['vad_filter']

        raise NotImplementedError('The WhisperX implementation is unfortunatly buggy at the moment, '
                                  'if you like to get it working with speechcatcher-data then remove this raise NotImplementedError statement. '
                                  'If you manage to fix the core dump please make a pull request!') 

    def load_model(self):
        self.model = whisperx.load_model("large-v3", device=self.device, compute_type='float16')

    def transcribe(self, url, language=None, duration=-1, params=None):
        if params is None:
            params = self.default_params
        if language is not None:
            params['language'] = language
        print('Running single-file transcription with CTranslate2 WhisperX fp16 implementation on:', url)
        #print('Beam size is:', params['beam_size'])
        audio = whisperx.load_audio(url)
        result = self.model.transcribe(audio, **params)
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


class WhisperCpp(WhisperSingleFile):
    '''A speechcatcher-data abstraction for pywhispercpp'''
    def __init__(self, model_name='large-v3', device='cuda', language='english', beam_size=5):
        super().__init__(model_name, device, language, beam_size)
        # Remove arguments that are unsupported by this implementation
        del self.default_params['fp16']
        del self.default_params['logprob_threshold']
        del self.default_params['suppress_tokens']
        del self.default_params['condition_on_previous_text']
        del self.default_params['compression_ratio_threshold']
        del self.default_params['no_speech_threshold']
        del self.default_params['best_of']
        # Add arguments that are specific to this implementation
        self.default_params['print_realtime'] = False
        self.default_params['print_progress'] = False

    def load_model(self):
        self.model = Model(self.model_name, device=self.device)

    def transcribe(self, url, language=None, duration=-1, params=None):
        if params is None:
            params = self.default_params
        if language is not None:
            params['language'] = language
        print('Running single-file transcription with pywhispercpp implementation on:', url)
        print('Beam size is:', params.get('beam_size', 'Not applicable'))

        segments = self.model.transcribe(url, **params)
        return {'segments': list(segments), 'language': language}

    def write_vtt(self, result, file):
        print("WEBVTT\n", file=file)
        for segment in result['segments']:
            print(
                f"{whisper.utils.format_timestamp(segment.start)} --> {whisper.utils.format_timestamp(segment.end)}\n"
                f"{segment.text.strip().replace('-->', '->')}\n",
                file=file,
                flush=True,
            )
