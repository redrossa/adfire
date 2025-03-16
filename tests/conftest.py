import pytest

from adfire.constants import RESOURCES_PATH


@pytest.fixture
def sample_path():
    return RESOURCES_PATH / 'sample'


@pytest.fixture
def sample_formatted_path():
    return RESOURCES_PATH / 'sample_formatted'
