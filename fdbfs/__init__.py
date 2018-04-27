import fdb

# XXX: can we avoid this here?
fdb.api_version(510)

from .fs import FdbFs

