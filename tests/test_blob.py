from unittest import TestCase
from fdbkit.blob import BlobWriter, BlobReader, BlobManager
import fdb


class BlobTestCase(TestCase):
    def setUp(self):
        self.db = fdb.open()
        self.directory = fdb.directory.create_or_open(self.db, ('blob-test',))

    def tearDown(self):
        del self.db[
            self.directory.range()
        ]
        self.db = None

    def _read_all(self, chunk_size=4):
        reader = BlobReader(self.db, self.directory, chunk_size)
        data = reader.read()
        reader.close()

        return data

    def test_manager(self):
        manager = BlobManager(self.db, ('blob-test',), 4)
        with manager.get_writer('test') as writer:
            writer.write('hello world')

        with manager.get_reader('test') as reader:
            self.assertEquals('hello world', reader.read())

        manager.write('test2', '12345')
        self.assertEquals(manager.read('test2'), '12345')

    def test_delete(self):
        manager = BlobManager(self.db, ('blob-test',), 4)
        self.assertFalse(manager.exists('test'))
        manager.write('test', '1111')
        self.assertTrue(manager.exists('test'))
        manager.delete('test')
        self.assertFalse(manager.exists('test'))

    def test_blob_writer(self):
        writer = BlobWriter(self.db, self.directory, 4)
        writer.write('abcd')
        writer.write('efg')

        self.assertEqual(7, writer.tell())

        self.assertEquals('abcdefg', self._read_all())

    def test_seek_write(self):
        writer = BlobWriter(self.db, self.directory, 4)
        writer.write('abcdefgh')

        writer.seek(5)
        writer.write('12345')

        self.assertEquals('abcde12345', self._read_all())

        # SEEK_CUR
        writer.seek(-1, 1)
        self.assertEqual(writer.tell(), 9)

    def test_seek_end(self):
        writer = BlobWriter(self.db, self.directory, 4)
        writer.write('12345')
        writer.seek(-1, 2)  # SEEK_END

        self.assertEqual(writer.tell(), 4)
        writer.write('abcd')
        writer.close()

        self.assertEquals('1234abcd', self._read_all())

    def test_outbound_seek_write(self):
        writer = BlobWriter(self.db, self.directory, 4)
        writer.write('abcdefgh')
        self.assertEquals(8, writer.tell())

        # can't go out of blob size
        writer.seek(100)
        self.assertEquals(8, writer.tell())

    def test_seek_read(self):
        writer = BlobWriter(self.db, self.directory, 4)
        writer.write('abcdefghijklmnop')

        self.assertEqual(16, writer.tell())

        reader = BlobReader(self.db, self.directory, 4)
        reader.seek(4)
        self.assertEquals('efghijklmnop', reader.read())

        reader.seek(2)
        self.assertEquals('cdefghijklmnop', reader.read())

    def test_large_chunk(self):
        writer = BlobWriter(self.db, self.directory, 4)

        for i in range(513):
            writer.write("1234")

        reader = BlobReader(self.db, self.directory, 4)
        reader.seek(2040)
        self.assertEqual(reader.read(), "123412341234")

    def test_closed_reader_writer(self):
        writer = BlobWriter(self.db, self.directory, 4)
        writer.write('abcdefg')
        writer.close()

        self.assertTrue(writer.closed)
        with self.assertRaises(IOError):
            writer.write('hij')

        reader = BlobReader(self.db, self.directory, 4)
        reader.close()
        self.assertTrue(reader.closed)
        with self.assertRaises(IOError):
            reader.read()
