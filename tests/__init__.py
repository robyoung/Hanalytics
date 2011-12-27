import json
import os

class TestHelpMixin(object):
    def fixture_path(self, *parts):
        return os.path.join(os.path.abspath(os.path.dirname(__file__)), 'fixtures', *parts)

    def read_fixture(self, *parts):
        fixture_path = self.fixture_path(*parts)
        extension = self._get_fixture_extension(fixture_path)
        with open(fixture_path) as f:
            return self._decode_fixture(f.read(), extension)

    def _get_fixture_extension(self, name):
        return name.rsplit(".", 1)[1]

    def _decode_fixture(self, value, extension):
        if extension == "json":
            return json.loads(value)
        elif extension == "jsonlines":
            return (json.loads(line.strip()) for line in value.split("\n") if line.strip())
        else:
            return value

    def removedirs(self, dir):
        if os.path.isdir(dir):
            for subdir in os.listdir(dir):
                self.removedirs(os.path.join(dir, subdir))
            os.rmdir(dir)
        else:
            os.unlink(dir)
