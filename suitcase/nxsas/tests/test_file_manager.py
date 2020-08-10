import os

import h5py

from suitcase import nxsas


def test_multi_file_manager_text_file(tmp_path):
    multi_file_manager = nxsas.FileManager(directory=tmp_path)

    multi_file_manager.open(content_desc="stream_data", relative_file_path="blah.txt", mode="x")
    multi_file_manager.close()

    file_list = os.listdir(tmp_path)
    print(file_list)
    assert len(file_list) == 1
    assert file_list == ["blah.txt"]


def test_multi_file_manager_hdf5_file(tmp_path):
    multi_file_manager = nxsas.FileManager(
        directory=tmp_path, allowed_modes={"w"}, open_file_fn=h5py.File
    )

    multi_file_manager.open(content_desc="stream_data", relative_file_path="blah.h5", mode="w")
    multi_file_manager.close()

    file_list = os.listdir(tmp_path)
    print(file_list)
    assert len(file_list) == 1
    assert file_list == ["blah.h5"]
