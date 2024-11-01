from contextlib import contextmanager


@contextmanager
def open_or_none(filename, mode='r'):
    try:
        f = open(filename, mode)
    except IOError as e:
        yield None, e
    else:
        try:
            yield f, None
        finally:
            f.close()
