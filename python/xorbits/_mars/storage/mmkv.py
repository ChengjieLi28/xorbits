import asyncio
import random
import shutil
import uuid
from typing import Dict, List, Tuple, Union

import cloudpickle
from xoscar.serialization import AioSerializer, deserialize
from xoscar.serialization.aio import BUFFER_SIZES_NAME, get_header_length

from ..utils import implements, lazy_import
from .base import ObjectInfo, StorageBackend, StorageLevel, register_storage_backend
from .core import StorageFileObject

ascii_lowercase = "abcdefghijklmnopqrstuvwxyz"
ascii_uppercase = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
ascii_letters = ascii_lowercase + ascii_uppercase
digits = "0123456789"


mmkv = lazy_import("mmkv")


@register_storage_backend
class MMKVStorage(StorageBackend):
    name = "mmkv"
    is_seekable = False

    def __init__(self):
        path = "".join(random.choice(ascii_letters + digits) for _ in range(8))
        mmkv.MMKV.initializeMMKV("/new_data2/lichengjie/mmkv")
        self._kv = mmkv.MMKV(path, mmkv.MMKVMode.MultiProcess)
        self._table = {path: self._kv}
        self._path = path
        # print(f"INIT storage backend: {os.getpid()}")

    @classmethod
    @implements(StorageBackend.setup)
    async def setup(cls, **kwargs) -> Tuple[Dict, Dict]:
        # path = kwargs.pop("path", None)
        if kwargs:  # pragma: no cover
            raise TypeError(f'MMKVStorage got unexpected config: {",".join(kwargs)}')
        # path = tempfile.mkdtemp(prefix="xmm")
        # print(f"Setup storage backend: {os.getpid()}")
        # print(f'Path: {path}')
        # mmkv.MMKV.initializeMMKV(path)

        return dict(), dict()

    @staticmethod
    def _get_path(obj_id: str) -> str:
        return obj_id.split("-")[0]
        # encoded = encoded_str.encode("utf8")
        # return base64.b64decode(encoded).decode("utf8")

    def _gen_key(self, key: str) -> str:
        _key = self._path + "-" + key
        return _key

    @staticmethod
    @implements(StorageBackend.teardown)
    async def teardown(**kwargs):
        path = kwargs.get("path")
        shutil.rmtree(path, ignore_errors=True)

    @property
    @implements(StorageBackend.level)
    def level(self):
        return StorageLevel.MEMORY

    @property
    @implements(StorageBackend.size)
    def size(self) -> Union[int, None]:
        return None

    @implements(StorageBackend.put)
    async def put(self, obj, importance=0) -> ObjectInfo:
        # print(f"PUT: {os.getpid()}")
        # start = time.time()
        string_id = str(uuid.uuid4())
        sid = self._gen_key(string_id)
        # print(f'Now put: {string_id}')
        serializer = AioSerializer(obj)
        buffers = await serializer.run()
        buf = buffers[0].tobytes()
        for buffer in buffers[1:]:
            buf = buf + buffer
        buffer_size = sum(getattr(buf, "nbytes", len(buf)) for buf in buffers)
        # print(f"Put here: {len(buf)}, {string_id}")
        # end1 = time.time()
        # self._kv.set(buf, string_id)
        await asyncio.to_thread(self._kv.set, buf, sid)
        # print(f'Pid: {os.getpid()}, Put key: {sid}')
        # end2 = time.time()
        # print(f"Put time cost: {end1 - start}, mmkv cost: {end2 - end1}, total: {end2 - start}")
        return ObjectInfo(size=buffer_size, object_id=sid)

    def _deserialize(self, path, object_id) -> object:
        # start = time.time()
        one_buf = self._table[path].getBytes(object_id)
        # end = time.time()
        # print(f"MMKV get cost: {end - start}")
        # print(f'Get buf: {len(one_buf)}, {object_id}')
        header_bytes = one_buf[:11]
        header_length = get_header_length(header_bytes)
        header = cloudpickle.loads(one_buf[11 : 11 + header_length])
        # get buffer size
        buffer_sizes = header[0].pop(BUFFER_SIZES_NAME)
        # get buffers
        buffers = []
        start = 11 + header_length
        for size in buffer_sizes:
            buffers.append(one_buf[start : start + size])
            start = start + size
        # get num of objs
        # num_objs = header[0].get("_N", 0)
        return deserialize(header, buffers)

        # if num_objs <= DEFAULT_SPAWN_THRESHOLD:
        #     return deserialize(header, buffers)
        # else:
        #     return await asyncio.to_thread(deserialize, header, buffers)

    @implements(StorageBackend.get)
    async def get(self, object_id, **kwargs) -> object:
        # print(f'Get {object_id}')
        # s = time.time()
        if kwargs:  # pragma: no cover
            raise NotImplementedError(f'Got unsupported args: {",".join(kwargs)}')
        path = self._get_path(object_id)
        if path not in self._table:
            # mmkv.MMKV.initializeMMKV('/tmp/xorbits')
            # print(f"First init mmkv Pid: {os.getpid()}, get key: {object_id}")
            kv = mmkv.MMKV(path, mmkv.MMKVMode.MultiProcess)
            self._table[path] = kv
        res = await asyncio.to_thread(self._deserialize, path, object_id)
        # try:
        #     print(f"Pid: {os.getpid()}, get key: {object_id}")
        #     res = await asyncio.to_thread(self._deserialize, path, object_id)
        # except Exception as e:
        #     print(f"Pid: {os.getpid()}, get key: {object_id}, err: {e}")
        #     raise
        # e = time.time()
        # print(f"Get total cost: {e - s}")
        return res

    @implements(StorageBackend.delete)
    async def delete(self, object_id):
        try:
            # print(f"Pid: {os.getpid()}, remove key: {object_id}")
            self._kv.remove(object_id)
        except:
            raise

    @implements(StorageBackend.object_info)
    async def object_info(self, object_id) -> ObjectInfo:
        # one_buf = self._kv.getBytes(object_id)
        # header_bytes = one_buf[:11]
        # header_length = get_header_length(header_bytes)
        # header = cloudpickle.loads(one_buf[11: 11 + header_length])
        # # get buffer size
        # buffer_sizes = header[0].pop(BUFFER_SIZES_NAME)
        # return ObjectInfo(size=11 + header_length + sum(buffer_sizes), object_id=object_id)
        return ObjectInfo(size=0, object_id=object_id)

    @implements(StorageBackend.list)
    async def list(self) -> List:  # pragma: no cover
        raise NotImplementedError("MMKV storage doesn't support `list` method.")

    @implements(StorageBackend.open_writer)
    async def open_writer(self, size=None) -> StorageFileObject:
        raise NotImplementedError("MMKV storage doesn't support `open_writer` method.")

    @implements(StorageBackend.open_reader)
    async def open_reader(self, object_id) -> StorageFileObject:
        raise NotImplementedError("MMKV storage doesn't support `open_reader` method.")
