import fdb
import uuid


CHUNK_SIZE = 10240  # 10K

# XXX: can we avoid this here?
fdb.api_version(510)


class FdbFs(object):
    def __init__(self, db, directory=None):
        self._db = db
        self._subspace = fdb.directory.create_or_open(db, directory or ('fdbs',))

        self._files = self._subspace['files']
        self._chunk = self._subspace['chunks']

    def open(self, filename, mode):
        chunk_id = self._open(self._db, filename)
        return FdbFsFile(self._db, self._chunk[chunk_id])

    @fdb.transactional
    def _open(self, tr, filename):
        """
        Allocate an unique chunkid for the file
        """
        filename_key = self._files.pack((filename,))

        if not tr[filename_key].present():
            chunk_id = str(uuid.uuid4())
            tr[filename_key] = chunk_id
        else:
            chunk_id = tr[filename_key]

        return chunk_id


class FdbFsFile(object):
    def __init__(self, db, space):
        self._db = db
        self._space = space
        self._cursor = 0
        self._chunk_index = 0

    def seek(self, cursor):
        pass

    def tell(self):
        return self._cursor

    def get_chunk_index(self):
        return self._chunk_index

    def write(self, data):
        self._write_chunk(self._db, self._chunk_index, data)

    def read(self, size=None):
        return self._read_chunk(self._db, self._chunk_index)

    def flush(self):
        # later
        pass

    def close(self):
        # just do nothing for now
        pass

    @fdb.transactional
    def _write_chunk(self, tr, chunk_index, chunk):
        assert len(chunk) <= CHUNK_SIZE
        tr[self._space.pack((chunk_index,))] = chunk

    @fdb.transactional
    def _read_chunk(self, tr, chunk_index):
        return tr[self._space.pack((chunk_index,))]
