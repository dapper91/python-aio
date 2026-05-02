import pathlib as pl
from typing import Protocol

import pytest


class ResourceGetter(Protocol):
    def __call__(self, name: str) -> bytes: ...


@pytest.fixture
def resource_dir(request: pytest.FixtureRequest) -> pl.Path:
    return pl.Path(request.fspath.dirname, 'resources')


@pytest.fixture
def resources(resource_dir) -> ResourceGetter:
    def getter(name: str) -> bytes:
        with (resource_dir / name).open('rb') as file:
            return file.read()

    return getter
