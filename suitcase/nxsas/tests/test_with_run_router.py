import os
from pathlib import Path

import pytest

import event_model

from suitcase import nxsas
from .test_sst_nexus_metadata import techniques_md

from suitcase.nxsas.tests.rsoxs_run_documents import (
    rsoxs_start_doc,
    rsoxs_descriptor_en_doc,
    rsoxs_event_page_en_doc,
)


# test the cases of presence and
# absence of techniques metadata
@pytest.mark.parametrize(
    "md", [techniques_md, {}],
)
def test_with_run_router(tmp_path, md):
    # use a directory that does not exist to test that it will be created
    output_dir_path = tmp_path / Path("doesnotexist")

    def factory(name, doc):
        serializer = nxsas.Serializer(
            file_prefix="doesnotexist/", directory=output_dir_path
        )
        return [serializer], []

    rr = event_model.RunRouter([factory])

    start_doc_md = {}
    start_doc_md.update(rsoxs_start_doc)
    start_doc_md.update(md)
    # compose_run will raise an exception if "time" and "uid" are in the metadata
    start_doc_md.pop("time")
    start_doc_md.pop("uid")
    (
        start_doc,
        compose_descriptor,
        compose_resource,
        compose_stop,
    ) = event_model.compose_run(metadata=start_doc_md)

    rr("start", start_doc)

    descriptor_doc_md = dict()
    descriptor_doc_md.update(rsoxs_descriptor_en_doc)
    # compose_descriptor will raise an exception if "run_start" is in the metadata
    descriptor_doc_md.pop("run_start")
    descriptor_doc, compose_event, compose_event_page = compose_descriptor(
        **descriptor_doc_md
    )
    rr("descriptor", descriptor_doc)

    event_md = dict()
    event_md.update(rsoxs_event_page_en_doc)
    # event_md["seq_num"] = [1]
    # the descriptor uid will interfere with compose_event
    event_md.pop("descriptor")
    event_doc = compose_event(**event_md)
    rr("event", event_doc)

    stop_doc = compose_stop()
    rr("stop", stop_doc)

    print(os.listdir(path=output_dir_path))
    assert len(os.listdir(path=output_dir_path)) == 1
