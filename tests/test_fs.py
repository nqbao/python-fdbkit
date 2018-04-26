from unittest import TestCase
from fdbfs import FdbFs
import fdb


class FsTestCase(TestCase):
    def setUp(self):
        self.db = fdb.open()
        self.directory = ('fdbfs-test',)
        print 123

    def tearDown(self):
        del self.db[
            fdb.directory.create_or_open(self.db, self.directory).range()
        ]
        self.db = None

    def test_read_write(self):
        fs = FdbFs(self.db, self.directory)

        f = fs.open('test.txt', 'w')
        f.write('Hello World')
        f.close()


        f = fs.open('test.txt', 'r')
        self.assertEquals('Hello World', f.read())
        f.close()
