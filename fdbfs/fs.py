import fdb
import uuid
import math


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

    def seek(self, cursor, whence=1):
        pass

    def tell(self):
        return self._cursor

    def write(self, data):
        # this is not thread-safe
        self._cursor = self._write(self._db, self._cursor, data)

    def read(self, size=None):
        return self._read_chunk(self._db, self._cursor)

    def flush(self):
        # later
        pass

    def close(self):
        # just do nothing for now
        pass

    @fdb.transactional
    def _write(self, tr, cursor, data):
        buf = buffer(data)

        chunks = int(math.ceil(float(len(buf)) / CHUNK_SIZE))
        chunk_index = cursor / CHUNK_SIZE
        start_cursor = chunk_index * CHUNK_SIZE

        # write the first partial chunk
        if start_cursor != cursor:
            next_cursor = start_cursor + CHUNK_SIZE
            chunk = tr[self._space.pack((chunk_index,))]

            # support this later
            assert chunk.present(), 'Do not support missing chunk yet'

            # TODO: we may cache this current chunk to avoid repeat reading
            new_chunk = chunk[0:cursor - start_cursor] + buf[0:next_cursor - cursor]
            tr[self._space.pack((chunk_index,))] = new_chunk

            buf = buffer(data, next_cursor - cursor)

            # advance the cursor
            if len(new_chunk) == CHUNK_SIZE:
                cursor = next_cursor
            else:
                cursor = start_cursor + len(new_chunk)

            if len(buf) == 0:
                return cursor

            return self._write(tr, cursor, buf)

        for i in range(chunks):
            chunk = data[i * CHUNK_SIZE:(i+1) * CHUNK_SIZE]
            chunk_size = len(chunk)
            assert chunk_size <= CHUNK_SIZE

            tr[self._space.pack((chunk_index,))] = chunk
            cursor += len(chunk)
            if chunk_size == CHUNK_SIZE:
                chunk_index += 1

        return cursor

    @fdb.transactional
    def _read_chunk(self, tr, cursor):
        chunk_index = cursor / CHUNK_SIZE
        return tr[self._space.pack((chunk_index,))]
