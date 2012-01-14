from hanalytics.writers import BaseWriter
from hanalytics.writers.mongo import SpeechWriter as MongoSpeechWriter
from hanalytics.writers.es import SpeechWriter as ESSpeechWriter

class SpeechWriter(BaseWriter):
    def __init__(self, index, type):
        self._mongo_writer = MongoSpeechWriter(index, type)
        self._es_writer = ESSpeechWriter(index, type)

    def pre_load(self):
        self._mongo_writer.pre_load()
        self._es_writer.pre_load()

    def save(self, document):
        self._mongo_writer.save(document)
        self._es_writer.save(document)

    def post_load(self):
        self._mongo_writer.post_load()
        self._es_writer.post_load()
