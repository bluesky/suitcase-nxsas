import contextlib
from pathlib import Path

import h5py
import pytest
from bluesky.tests.conftest import RE  # noqa
from ophyd.tests.conftest import hw  # noqa
# from suitcase.utils.tests.conftest import (  # noqa
#     example_data,
#     generate_data,
#     plan_type,
#     detector_list,
#     event_type,
# )  # noqa


@pytest.fixture
def tmp_h5_file(tmp_path):
    return h5py.File(tmp_path / Path("test.h5"), "w")


@pytest.fixture
def h5_context(tmp_path):
    @contextlib.contextmanager
    def make_context():
        print("  entering")
        try:
            f = h5py.File(tmp_path / Path("test.h5"), "w")
            yield f
        except RuntimeError as err:
            print("  ERROR:", err)
        finally:
            print("  exiting")
            f.close()

    return make_context
