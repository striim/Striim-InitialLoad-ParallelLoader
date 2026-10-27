"""The documented entry points run directly (./manage.sh, ./quickstart.sh), so they must stay executable."""
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.mark.skipif(sys.platform == "win32", reason="no executable bit on Windows")
@pytest.mark.parametrize("path", ["manage.sh", "manage.py", "quickstart/postgres/quickstart.sh"])
def test_entry_point_is_executable(path):
    assert os.access(os.path.join(ROOT, path), os.X_OK), f"{path} lost its executable bit"
