import pyes
import hashlib

from hanalytics.writers import BaseWriter

class Writer(BaseWriter):
    @property
    def client(self):
        if not hasattr(self, "_client"):
            self._client = pyes.ES("localhost:9200", timeout=30.0)
        return self._client

    def save(self, document):
        self.client.index(document, self._index, self._type)

    def create_index(self):
        self.client.create_index_if_missing(self._index)

    def put_mapping(self, mapping):
        self.client.put_mapping(self._type, {"properties":mapping}, [self._index])

class SpeechWriter(Writer):
    def pre_load(self):
        self.create_index()
        self.put_mapping({
            "house": {"type":"string", "index":"not_analyzed"},
            "speakerid": {"type":"string", "index":"not_analyzed"},
            "speakername": {"type":"string", "index":"not_analyzed"},
            "column": {"type":"string", "index":"not_analyzed"},
            "date": {"type":"date", "format":"YYYY-MM-dd"},
            "url":{"type":"string", "index":"not_analyzed"}
        })
        del self._client

    def save(self, document):
        d = document['date']
        document['date'] = "%s-%02i-%02i" % (d.year, d.month, d.day)
        # todo: think about html stripping
        document['text'] = "".join(document['text'])
        m = hashlib.md5()
        m.update(document['id'])
        self.client.index(document, self._index, self._type, id=m.hexdigest(), bulk=True)
