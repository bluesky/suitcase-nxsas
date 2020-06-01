from suitcase.sas import _copy_metadata_to_h5_datasets


def test_str(h5_context):
    with h5_context() as tmp_h5_file:
        _copy_metadata_to_h5_datasets(
            a_mapping={"uid": "5720b93f-2f5d-4ace-8ace-2efb669fc38f"},
            h5_group=tmp_h5_file,
        )

        assert "uid" in tmp_h5_file
        assert tmp_h5_file["uid"][()] == "5720b93f-2f5d-4ace-8ace-2efb669fc38f"


def test_str_list(h5_context):
    with h5_context() as tmp_h5_file:
        _copy_metadata_to_h5_datasets(
            a_mapping={"detectors": ["Synced", "en_energy"]}, h5_group=tmp_h5_file
        )

        assert "detectors" in tmp_h5_file
        print("@@@ detectors dataset:")
        print(tmp_h5_file["detectors"])
        assert all(tmp_h5_file["detectors"][()] == ["Synced", "en_energy"])


def test_str_tuple(h5_context):
    with h5_context() as tmp_h5_file:
        _copy_metadata_to_h5_datasets(
            a_mapping={"detectors": ("Synced", "en_energy")}, h5_group=tmp_h5_file
        )

        assert "detectors" in tmp_h5_file
        print("@@@ detectors dataset:")
        print(tmp_h5_file["detectors"])
        assert all(tmp_h5_file["detectors"][()] == ("Synced", "en_energy"))
