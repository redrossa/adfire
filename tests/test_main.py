import sys

import pytest

from adfire.__main__ import main


@pytest.mark.parametrize('option', [
    '-h', '--help',
    '-v', '--version',
])
def test_option_exists(option):
    sys.argv = ['adfire', option]
    with pytest.raises(SystemExit, match='0'):
        main()


class TestModes:
    modes = ['init', 'lint', 'format']

    @pytest.mark.parametrize('mode', modes)
    def test_mode_exists(self, mode):
        sys.argv = ['adfire', mode]
        main()

    def test_mode_not_exists(self):
        sys.argv = ['adfire', 'idk']
        with pytest.raises(SystemExit, match='2'):
            main()
