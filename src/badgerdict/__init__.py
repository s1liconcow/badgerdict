import ctypes
import os
import pickle
import tempfile
import threading
from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Any, ClassVar, Optional, Union

try:  # POSIX-only import guarded for portability.
    import fcntl  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover - Windows fallback handled separately.
    fcntl = None

try:  # Windows-specific lock helpers.
    import msvcrt  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover
    msvcrt = None


BytesLike = Union[bytes, bytearray, memoryview, str]
_MISSING = object()
_VALUE_RAW = 0x00
_VALUE_STR = 0x01
_VALUE_PICKLED = 0x02


class _FileLock:
    """Minimal cross-platform advisory file lock for inter-process coordination."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._fh: Optional[Any] = None

    def acquire(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self._path, os.O_RDWR | os.O_CREAT, 0o666)
        self._fh = os.fdopen(fd, "r+b", buffering=0)
        if fcntl is not None:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        elif msvcrt is not None:  # pragma: no cover - Windows only
            # Ensure there is at least one byte to lock on Windows.
            if self._fh.tell() == 0:
                self._fh.write(b"\0")
                self._fh.flush()
            msvcrt.locking(self._fh.fileno(), msvcrt.LK_LOCK, 1)
        else:  # pragma: no cover - platforms without locking support
            raise RuntimeError("file locking is not supported on this platform")

    def release(self) -> None:
        if not self._fh:
            return
        try:
            if fcntl is not None:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            elif msvcrt is not None:  # pragma: no cover
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            self._fh.close()
            self._fh = None

    def __enter__(self) -> "_FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


__all__ = ["BadgerDict", "BadgerError", "PersistentObject"]


class BadgerError(Exception):
    """Raised when the underlying Badger interaction fails."""


class BadgerDict:
    """Minimal dictionary-style wrapper backed by Badger through a Go shared library."""

    _init_lock = threading.Lock()
    _lib: Optional[ctypes.CDLL] = None

    def __init__(
        self,
        path: Optional[str] = None,
        *,
        in_memory: bool = False,
        lib_path: Optional[str] = None,
        auto_pickle: bool = True,
    ) -> None:
        self._ensure_library(lib_path)
        self._handle = self._open(path, in_memory)
        self._auto_pickle = auto_pickle

    @classmethod
    def _ensure_library(cls, lib_path: Optional[str]) -> None:
        if cls._lib is not None:
            return
        with cls._init_lock:
            if cls._lib is not None:
                return
            inferred_path = lib_path or cls._default_library_path()
            cls._lib = ctypes.CDLL(inferred_path)
            cls._configure_signatures()

    @classmethod
    def _configure_signatures(cls) -> None:
        assert cls._lib is not None
        lib = cls._lib
        lib.Open.argtypes = [ctypes.c_char_p, ctypes.c_int]
        lib.Open.restype = ctypes.c_size_t

        lib.Close.argtypes = [ctypes.c_size_t]
        lib.Close.restype = ctypes.c_int

        lib.Set.argtypes = [ctypes.c_size_t, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_int]
        lib.Set.restype = ctypes.c_int

        lib.Get.argtypes = [ctypes.c_size_t, ctypes.c_char_p, ctypes.c_int, ctypes.POINTER(ctypes.c_int)]
        lib.Get.restype = ctypes.c_void_p

        lib.Delete.argtypes = [ctypes.c_size_t, ctypes.c_char_p, ctypes.c_int]
        lib.Delete.restype = ctypes.c_int

        lib.Sync.argtypes = [ctypes.c_size_t]
        lib.Sync.restype = ctypes.c_int

        lib.LastError.argtypes = []
        lib.LastError.restype = ctypes.c_void_p

        lib.FreeCString.argtypes = [ctypes.c_void_p]
        lib.FreeCString.restype = None

        lib.FreeBuffer.argtypes = [ctypes.c_void_p]
        lib.FreeBuffer.restype = None

    @staticmethod
    def _default_library_path() -> str:
        base_dir = os.path.dirname(__file__)
        suffix = {
            "win32": ".dll",
            "darwin": ".dylib",
        }.get(os.sys.platform, ".so")
        return os.path.join(base_dir, f"libbadgerdict{suffix}")

    @classmethod
    def _last_error(cls) -> Optional[str]:
        assert cls._lib is not None
        err_ptr = cls._lib.LastError()
        if not err_ptr:
            return None
        try:
            msg = ctypes.string_at(err_ptr).decode("utf-8", "replace")
        finally:
            cls._lib.FreeCString(err_ptr)
        return msg or None

    @classmethod
    def _check_status(cls, status: int) -> None:
        if status == 0:
            return
        msg = cls._last_error() or "unknown badger error"
        raise BadgerError(msg)

    @classmethod
    def _open(cls, path: Optional[str], in_memory: bool) -> int:
        assert cls._lib is not None
        if in_memory:
            encoded_path = b""
        else:
            if not path:
                raise ValueError("A filesystem path is required unless in_memory=True")
            encoded_path = path.encode("utf-8")
        handle = cls._lib.Open(encoded_path, int(bool(in_memory)))
        if handle == 0:
            msg = cls._last_error() or "failed to open badger dictionary"
            raise BadgerError(msg)
        return int(handle)

    def __getitem__(self, key: Any) -> Any:
        return self.get(key, raise_missing=True)

    def __setitem__(self, key: Any, value: Any) -> None:
        self.set(key, value)

    def __delitem__(self, key: Any) -> None:
        if not self.delete(key):
            raise KeyError(key)

    def __contains__(self, key: Any) -> bool:
        result = self.get(key, default=_MISSING)
        return result is not _MISSING

    def __enter__(self) -> "BadgerDict":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _call(self, func_name: str, *args) -> int:
        if self._handle == 0:
            raise BadgerError("badger dictionary is closed")
        assert self._lib is not None
        func = getattr(self._lib, func_name)
        return func(*args)

    def _encode_key(self, key: Any) -> bytes:
        if isinstance(key, (bytes, bytearray, memoryview)):
            data = bytes(key)
        elif isinstance(key, str):
            data = key.encode("utf-8")
        else:
            data = pickle.dumps(key, protocol=pickle.HIGHEST_PROTOCOL)
        if not data:
            raise ValueError("empty keys are not supported")
        return data

    def _encode_value(self, value: Any) -> bytes:
        if isinstance(value, (bytes, bytearray, memoryview)):
            payload = bytes(value)
            return bytes([_VALUE_RAW]) + payload
        if isinstance(value, str):
            payload = value.encode("utf-8")
            return bytes([_VALUE_STR]) + payload
        if not self._auto_pickle:
            raise TypeError(f"Value type {type(value)!r} is not bytes/str and auto_pickle=False.")
        payload = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        return bytes([_VALUE_PICKLED]) + payload

    def _decode_value(self, data: bytes) -> Any:
        if not data:
            return b""
        type_tag = data[0]
        payload = data[1:]
        if type_tag == _VALUE_RAW:
            return payload
        if type_tag == _VALUE_STR:
            return payload.decode("utf-8")
        if type_tag == _VALUE_PICKLED:
            return pickle.loads(payload)
        return data

    def set(self, key: Any, value: Any) -> None:
        key_bytes = self._encode_key(key)
        value_bytes = self._encode_value(value)
        status = self._call(
            "Set",
            ctypes.c_size_t(self._handle),
            ctypes.c_char_p(key_bytes),
            ctypes.c_int(len(key_bytes)),
            ctypes.c_char_p(value_bytes),
            ctypes.c_int(len(value_bytes)),
        )
        self._check_status(status)

    def get(self, key: Any, default: Any = None, *, raise_missing: bool = False) -> Any:
        key_bytes = self._encode_key(key)
        value_len = ctypes.c_int()
        ptr = self._call(
            "Get",
            ctypes.c_size_t(self._handle),
            ctypes.c_char_p(key_bytes),
            ctypes.c_int(len(key_bytes)),
            ctypes.byref(value_len),
        )

        if not ptr and value_len.value == 0:
            msg = self._last_error()
            if msg:
                if "not found" in msg.lower():
                    if raise_missing:
                        raise KeyError(key)
                    return default
                raise BadgerError(msg)
            if raise_missing:
                raise KeyError(key)
            return default

        try:
            raw = ctypes.string_at(ptr, value_len.value)
        finally:
            self._lib.FreeBuffer(ptr)
        return self._decode_value(raw)

    def delete(self, key: Any) -> bool:
        key_bytes = self._encode_key(key)
        status = self._call(
            "Delete",
            ctypes.c_size_t(self._handle),
            ctypes.c_char_p(key_bytes),
            ctypes.c_int(len(key_bytes)),
        )
        if status == 0:
            return True
        msg = self._last_error()
        if msg and "not found" in msg.lower():
            return False
        self._check_status(status)
        return True

    def sync(self) -> None:
        status = self._call("Sync", ctypes.c_size_t(self._handle))
        self._check_status(status)

    def close(self) -> None:
        if self._handle == 0:
            return
        status = self._call("Close", ctypes.c_size_t(self._handle))
        self._handle = 0
        if status != 0:
            self._check_status(status)

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class PersistentObject:
    """Base class for Badger-backed persistent records with inter-process safety.

    Subclasses should override :meth:`to_record` / :meth:`from_record` when the
    default dictionary representation is insufficient. Storage is configured per
    subclass via :meth:`configure_storage` and each operation acquires a file
    lock so processes coordinate access safely.
    """

    _storage_path: ClassVar[Optional[Path]] = None
    _storage_in_memory: ClassVar[bool] = False
    _storage_lib_path: ClassVar[Optional[str]] = None
    _storage_auto_pickle: ClassVar[bool] = True
    _lock_path: ClassVar[Optional[Path]] = None
    _namespace: ClassVar[Optional[str]] = None

    def __init__(self, key: Any) -> None:
        self.key = key

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------
    @classmethod
    def configure_storage(
        cls,
        path: Optional[str],
        *,
        in_memory: bool = False,
        lib_path: Optional[str] = None,
        auto_pickle: bool = True,
        lock_path: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> None:
        """Configure the Badger datastore backing this subclass.

        Args:
            path: Filesystem path for the Badger database. Required unless
                ``in_memory`` is True.
            in_memory: Whether to use an in-memory Badger instance.
            lib_path: Optional override pointing at the compiled shared library.
            auto_pickle: Whether values should be automatically pickled.
            lock_path: Optional explicit path to a file used for inter-process
                locking. Defaults to ``<path>.lock`` or a temp file for in-memory
                stores.
            namespace: Optional namespace prefix for keys. Defaults to the
                class name.
        """

        if not in_memory:
            if not path:
                raise ValueError("path is required when using persistent storage")
            resolved = Path(path).expanduser().resolve()
            resolved.mkdir(parents=True, exist_ok=True)
        else:
            resolved = None

        cls._storage_path = resolved
        cls._storage_in_memory = in_memory
        cls._storage_lib_path = lib_path
        cls._storage_auto_pickle = auto_pickle
        cls._namespace = namespace or cls.__name__

        if lock_path:
            cls._lock_path = Path(lock_path).expanduser().resolve()
        elif in_memory:
            temp_dir = Path(tempfile.gettempdir())
            cls._lock_path = temp_dir / f"badgerdict-{cls.__name__}.lock"
        elif resolved is not None:
            cls._lock_path = resolved / f".{cls.__name__.lower()}.lock"
        else:
            cls._lock_path = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def save(self) -> "PersistentObject":
        """Persist the current state."""

        cls = type(self)

        def _writer(_: "PersistentObject") -> "PersistentObject":
            return self

        cls.update(self.key, default_factory=lambda: self, mutator=_writer)
        return self

    @classmethod
    def load(cls, key: Any, default: Any = _MISSING) -> "PersistentObject":
        """Load an instance by key.

        Args:
            key: Identifier originally supplied to the constructor.
            default: Optional fallback returned when the key is missing. If the
                default is not provided a :class:`KeyError` is raised.
        """

        record = cls._get_record(key)
        if record is _MISSING:
            if default is _MISSING:
                raise KeyError(key)
            return default
        return cls.from_record(key, record)

    @classmethod
    def exists(cls, key: Any) -> bool:
        return cls._get_record(key) is not _MISSING

    @classmethod
    def delete(cls, key: Any) -> bool:
        cls._ensure_configured()
        full_key = cls._format_key(key)
        with cls._locked_store() as store:
            return store.delete(full_key)

    @classmethod
    def update(
        cls,
        key: Any,
        *,
        default_factory: Optional[Any] = None,
        mutator: Optional[Any] = None,
    ) -> "PersistentObject":
        """Atomically load, mutate, and persist an object.

        ``mutator`` receives the current object (creating one via
        ``default_factory`` when missing). Returning ``None`` implies in-place
        mutation and the same object is re-written.
        """

        cls._ensure_configured()
        full_key = cls._format_key(key)

        with cls._locked_store() as store:
            record = store.get(full_key, default=_MISSING)
            if record is _MISSING:
                if default_factory is None:
                    raise KeyError(key)
                candidate = default_factory() if callable(default_factory) else default_factory
                if not isinstance(candidate, cls):
                    raise TypeError("default_factory must produce an instance of the subclass")
                candidate.key = key
                current = candidate
            else:
                current = cls.from_record(key, record)

            if mutator is not None:
                updated = mutator(current)
                if updated is not None:
                    current = updated

            if not isinstance(current, cls):
                raise TypeError("mutator must return an instance of the subclass or None")

            store[full_key] = current.to_record()
            return current

    # ------------------------------------------------------------------
    # Extensibility hooks
    # ------------------------------------------------------------------
    def to_record(self) -> Any:
        """Convert the instance to a storable representation."""

        payload = dict(self.__dict__)
        payload.pop("key", None)
        return payload

    @classmethod
    def from_record(cls, key: Any, record: Any) -> "PersistentObject":
        instance = cls.__new__(cls)
        cls.__init__(instance, key)
        if isinstance(record, dict):
            instance.__dict__.update(record)
        else:
            instance.value = record
        return instance

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    @classmethod
    def _ensure_configured(cls) -> None:
        if cls._storage_path is None and not cls._storage_in_memory:
            raise RuntimeError("PersistentObject storage is not configured")

    @classmethod
    def _format_key(cls, key: Any) -> bytes:
        namespace = cls._namespace or cls.__name__
        return pickle.dumps((namespace, key), protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    @contextmanager
    def _locked_store(cls):
        cls._ensure_configured()
        lock_cm = cls._lock_context()
        with lock_cm:
            with cls._open_store() as store:
                yield store

    @classmethod
    def _lock_context(cls):
        if cls._lock_path is None:
            return nullcontext()
        return _FileLock(cls._lock_path)

    @classmethod
    def _open_store(cls):
        if cls._storage_in_memory:
            path = None
        elif cls._storage_path is not None:
            path = str(cls._storage_path)
        else:
            path = None
        return BadgerDict(
            path,
            in_memory=cls._storage_in_memory,
            lib_path=cls._storage_lib_path,
            auto_pickle=cls._storage_auto_pickle,
        )

    @classmethod
    def _get_record(cls, key: Any) -> Any:
        cls._ensure_configured()
        full_key = cls._format_key(key)
        with cls._locked_store() as store:
            result = store.get(full_key, default=_MISSING)
        return result
