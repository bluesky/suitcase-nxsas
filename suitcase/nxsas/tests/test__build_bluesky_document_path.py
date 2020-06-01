from suitcase.nxsas import _parse_bluesky_document_path


def test__build_bluesky_document_path():
    parsed_path = _parse_bluesky_document_path("#bluesky/start@abc")
    assert parsed_path["doc"] == "start"
    assert parsed_path["attribute"] == "abc"

    parsed_path = _parse_bluesky_document_path("#bluesky/start/abc")
    assert parsed_path["doc"] == "start"
    assert parsed_path["keys"] == ("abc",)

    parsed_path = _parse_bluesky_document_path("#bluesky/start/abc/def")
    assert parsed_path["doc"] == "start"
    assert parsed_path["keys"] == ("abc", "def")

    parsed_path = _parse_bluesky_document_path("#bluesky/start/abc/def@ghi")
    assert parsed_path["doc"] == "start"
    assert parsed_path["keys"] == ("abc", "def")
    assert parsed_path["attribute"] == "ghi"

    parsed_path = _parse_bluesky_document_path("#bluesky/desc/primary/abc/def@ghi")
    assert parsed_path["doc"] == "desc"
    assert parsed_path["stream"] == "primary"
    assert parsed_path["keys"] == ("abc", "def")
    assert parsed_path["attribute"] == "ghi"

    parsed_path = _parse_bluesky_document_path("#bluesky/stop/abc/def@ghi")
    assert parsed_path["doc"] == "stop"
    assert parsed_path["keys"] == ("abc", "def")
    assert parsed_path["attribute"] == "ghi"
