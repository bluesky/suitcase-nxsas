import os

import event_model
from event_model import RunRouter

from suitcase.nxsas import Serializer as Serializer_NXSAS
from .test_sst_nexus_metadata import techniques_md

from suitcase.nxsas.tests.rsoxs_run_documents import (
    rsoxs_start_doc,
    rsoxs_descriptor_en_doc,
    rsoxs_event_page_en_doc,
)


def test_(tmp_path):

    def factory(name, doc):

        serializer = Serializer_NXSAS(directory=tmp_path)

        return [serializer], []

    rr = RunRouter([factory])

    start_doc_md = {}
    start_doc_md.update(rsoxs_start_doc)
    start_doc_md.update(techniques_md)
    # compose_run will throw an exception if "time" and "uid" are in the metadata
    start_doc_md.pop("time")
    start_doc_md.pop("uid")
    documents = []
    (
        start_doc,
        compose_descriptor,
        compose_resource,
        compose_stop,
    ) = event_model.compose_run(
        # 'run start' document
        metadata=start_doc_md
    )

    #documents.append(("start", start_doc))
    rr("start", start_doc)

    descriptor_doc_md = dict()
    descriptor_doc_md.update(rsoxs_descriptor_en_doc)
    # compose_descriptor will raise an exception if "run_start" is in the metadata
    descriptor_doc_md.pop("run_start")
    descriptor_doc, compose_event, compose_event_page = compose_descriptor(
        **descriptor_doc_md
    )
    #documents.append(("descriptor", descriptor_doc))
    rr("descriptor", descriptor_doc)

    event_md = dict()
    event_md.update(rsoxs_event_page_en_doc)
    # event_md["seq_num"] = [1]
    # the descriptor uid will interfere with compose_event
    event_md.pop("descriptor")
    event_doc = compose_event(**event_md)
    #documents.append(("event", event_doc))
    rr("event", event_doc)

    stop_doc = compose_stop()
    #documents.append(("stop", stop_doc))
    rr("stop", stop_doc)

    #rr("start", rsoxs_start_doc)
    #rr("descriptor", rsoxs_descriptor_en_doc)
    #rr("event_page", rsoxs_event_page_en_doc)

    # was anything written?
    print(os.listdir(path=tmp_path))
    len(os.listdir(path=tmp_path)) == 1
