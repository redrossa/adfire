import uuid

import pytest


@pytest.fixture
def id_generator():
    def _generate(seed: str = ''):
        def generate_id():
            nonlocal seed
            id = uuid.uuid5(uuid.NAMESPACE_DNS, f'{seed}{str(generate_id.counter)}')
            generate_id.counter += 1
            return id
        generate_id.counter = 0
        return generate_id
    return _generate