import pytest

from skyshelve import SkyShelve


def test_default_factory_populates_missing(shared_library):
    calls = {"count": 0}

    def factory():
        calls["count"] += 1
        return {"value": calls["count"]}

    store = SkyShelve(None, in_memory=True, lib_path=str(shared_library), default_factory=factory)
    try:
        created = store["missing"]
        assert created == {"value": 1}
        assert calls["count"] == 1

        retrieved = store["missing"]
        assert retrieved == {"value": 1}
        assert calls["count"] == 1, "default_factory should only run once for a given key"

        assert store.get("missing") == {"value": 1}
    finally:
        store.close()


def test_default_factory_attribute_can_be_mutated(shared_library):
    store = SkyShelve(None, in_memory=True, lib_path=str(shared_library))
    try:
        with pytest.raises(KeyError):
            _ = store["missing"]

        store.default_factory = lambda: 41
        assert store["missing"] == 41
        assert store.get("missing") == 41
    finally:
        store.close()
