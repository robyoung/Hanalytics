"""
Writer classes for writing items into a MongoDB instance.
"""
import copy
import pymongo

from hanalytics.writers import BaseWriter

class Writer(BaseWriter):
    @property
    def client(self):
        if not hasattr(self, "_client"):
            self._client = pymongo.Connection()[self._index][self._type]
        return self._client

    def save(self, document):
        self.client.save(document)

class SpeechWriter(Writer):
    def pre_load(self):
        """
        Drop indexes and remove any cached collection object.
        """
        # speed up the load by dropping the indexes and reapplying them after
        self.client.drop_index("timestamp")
        self.client.drop_index("house_timestamp")
        del self._client

    def save(self, document):
        document = copy.deepcopy(document)
        document['_id'] = document['id']
        del document['id']
        super(SpeechWriter, self).save(document)

    def post_load(self):
        self.client.ensure_index("timestamp", pymongo.ASCENDING, name="timestamp")
        self.client.ensure_index([("house", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING)], name="house_timestamp")

