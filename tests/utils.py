import filecmp
from contextlib import contextmanager
from pathlib import Path


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


def dir_is_equal(dir1: Path, dir2: Path) -> bool:
    """
    Compare two directories recursively to check if they contain the same items.

    Parameters:
    - dir1: Path to the first directory
    - dir2: Path to the second directory

    Returns:
    - True if both directories contain the same items (files and subfolders).
    - False if they differ.
    """
    # Check if both directories exist
    if not dir1.exists() or not dir2.exists():
        return False

    # Check if both directories are actually directories
    if not dir1.is_dir() or not dir2.is_dir():
        return False

    # Get lists of all files and directories in both dirs
    dir1_items = {item.relative_to(dir1) for item in dir1.rglob('*')}
    dir2_items = {item.relative_to(dir2) for item in dir2.rglob('*')}

    # Compare the sets of items
    if dir1_items != dir2_items:
        return False

    # Compare the contents of the files if the structure is the same
    for item in dir1_items:
        file1 = dir1 / item
        file2 = dir2 / item

        # If it's a file, compare their contents
        if file1.is_file() and file2.is_file():
            if not filecmp.cmp(file1, file2, shallow=False):
                return False
        # If it's a directory, just continue (no further content to check)
        elif file1.is_dir() and file2.is_dir():
            continue

    return True
