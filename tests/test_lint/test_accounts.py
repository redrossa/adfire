from adfire.lint import AccountLinter
from adfire.utils import dict_to_namespace


class TestAccountLinter:
    class TestGetUserNames:
        def test_verbose(self):
            config = [
                {
                    'names': ['Amex Gold'],
                    'masks': ['51001', '51019', '51012'],
                }
            ]
            namespace = dict_to_namespace(config)
            actual = AccountLinter.get_name_mask_pairs(namespace)
            expected = [
                ('Amex Gold', '51001'),
                ('Amex Gold', '51019'),
                ('Amex Gold', '51012'),
            ]
            assert actual == expected

        def test_no_mask(self):
            config = [
                {
                    'names': ['Alight 401k'],
                }
            ]
            namespace = dict_to_namespace(config)
            actual = AccountLinter.get_name_mask_pairs(namespace)
            expected = [
                ('Alight 401k', None),
            ]
            assert actual == expected
