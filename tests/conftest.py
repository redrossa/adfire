from pathlib import Path

import pytest


@pytest.fixture
def resources_path():
    return Path(__file__).parent.parent / 'resources'


@pytest.fixture
def sample_path(resources_path):
    return resources_path / 'sample'
