import numpy as np
import pandas as pd
import pytest

from adfire.config import RESOURCES_PATH


@pytest.fixture
def sample_path():
    return RESOURCES_PATH / 'sample'


@pytest.fixture
def sample_formatted_path():
    return RESOURCES_PATH / 'sample_formatted'
