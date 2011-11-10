
class BaseWriter(object):
    def __init__(self, index, type):
        self._index = index
        self._type = type

    def pre_load(self):
        pass

    def post_load(self):
        pass

    def save(self, document):
        raise NotImplementedError()
