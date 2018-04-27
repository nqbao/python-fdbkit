import fdb
import uuid


from .blob import BlobWriter, BlobReader


CHUNK_SIZE = 10240  # 10K


class FdbFs(object):
    def __init__(self, db, directory=None):
        self._db = db
        self._subspace = fdb.directory.create_or_open(db, directory or ('fdbs',))

        self._files = self._subspace['files']
        self._blobs = self._subspace['blobs']

    def open(self, filename, mode):
        blob_id = self._open(self._db, filename)
        return FdbFsFile(self._db, self._blobs[blob_id])

    @fdb.transactional
    def _open(self, tr, filename):
        """
        Allocate an unique blob id for the file
        """
        # TODO: use fdb instead of key prefix directory
        filename_key = self._files.pack((filename,))

        if not tr[filename_key].present():
            blob_id = str(uuid.uuid4())
            tr[filename_key] = blob_id
        else:
            blob_id = tr[filename_key]

        return blob_id


class FdbFsFile(object):
    def __init__(self, db, space):
        self._db = db
        self._space = space
        self._cursor = 0
        self._writer = BlobWriter(db, space, CHUNK_SIZE)
        self._reader = BlobReader(db, space, CHUNK_SIZE)

    def seek(self, cursor, whence=1):
        pass

    def tell(self):
        return self._writer.tell()

    def write(self, data):
        # TODO: sync reader & writer cursor
        return self._writer.write(data)

    def read(self, size=None):
        # TODO: sync reader & writer cursor
        return self._reader.read(size)

    def flush(self):
        # later
        pass

    def close(self):
        self._writer.close()
        self._reader.close()
