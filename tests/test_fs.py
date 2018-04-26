from unittest import TestCase
from fdbfs import FdbFs
import fdb


LARGE_STRING = '\0' * 1024 * 10

class FsTestCase(TestCase):
    def setUp(self):
        self.db = fdb.open()
        self.directory = ('fdbfs-test',)
        self.fs = FdbFs(self.db, self.directory)

    def tearDown(self):
        del self.db[
            fdb.directory.create_or_open(self.db, self.directory).range()
        ]
        self.db = None

    def test_multi_write(self):
        fs = self.fs
        f = fs.open('test.txt', 'w')
        f.write('Hello')
        f.write('World')

        self.assertEqual(f.tell(), 10)
        f.close()

        f = fs.open('test.txt', 'r')
        self.assertEqual('HelloWorld', f.read())
        f.close()

    def test_large_write(self):
        fs = self.fs
        f = fs.open('test.txt', 'w')
        f.write(LARGE_STRING)
        f.write('1')
        f.write('1')
        f.write(LARGE_STRING)
        self.assertEquals(20482, f.tell())
        f.close()

    def test_read_write(self):
        fs = self.fs

        f = fs.open('test.txt', 'w')
        self.assertEqual(0, f.tell())
        f.write('HelloWorld')
        self.assertEqual(10, f.tell())
        f.close()


        f = fs.open('test.txt', 'r')
        self.assertEquals('HelloWorld', f.read())
        f.close()
