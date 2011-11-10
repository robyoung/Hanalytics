import pymongo

from hanalytics.writers import BaseWriter

class Writer(BaseWriter):
    @property
    def client(self):
        if not hasattr(self, "_client"):
            self._client = pymongo.Connection()[self._index][self._type]
        return self._client

    def save(self, document):
        self._client.insert(document)

class SpeechWriter(Writer):
    def pre_load(self):
        # speed up the load by dropping the indexes and reapplying them after
        self.client.dropIndex("timestamp", pymongo.ASCENDING)
        self.client.dropIndex([("house", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING)])
        del self._client

    def save(self, document):
        document['_id'] = document['id']
        del document['id']
        super(SpeechWriter, self).save(document)

    def post_load(self):
        self.client.ensureIndex("timestamp", pymongo.ASCENDING)
        self.client.ensureIndex([("house", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING)])

