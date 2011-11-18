import os

class TestHelpMixin(object):
    def fixture_path(self, *parts):
        return os.path.join(os.path.abspath(os.path.dirname(__file__)), 'fixtures', *parts)

    def read_fixture(self, *parts):
        with open(self.fixture_path(*parts)) as f:
            return f.read()

    def removedirs(self, dir):
        if os.path.isdir(dir):
            for subdir in os.listdir(dir):
                self.removedirs(os.path.join(dir, subdir))
            os.rmdir(dir)
        else:
            os.unlink(dir)
