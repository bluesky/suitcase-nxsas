import logging

import h5py

import event_model

from suitcase.nxsas import export
from suitcase.nxsas.tests.rsoxs_run_documents import (
    rsoxs_start_doc,
    rsoxs_descriptor_en_doc,
    rsoxs_event_page_en_doc,
)


techniques_md = {
    "md": {
        "techniques": [
            # SAXS technique
            {
                "version": 1,
                "technique": "SAXS",
                "nxsas": {
                    "entry": {
                        "_attributes": {"NX_Class": "NXEntry", "default": "data"},
                        "end_time": {
                            "_attributes": {
                                "NDAttrDescription": "image ending time",
                                "NDAttrName": "EndTime",
                                "NDAttrSource": "91dcLAX:SAXS:EndExposureTime",
                                "NDAttrSourceType": "NDAttrSourceEPICSPV",
                            },
                            "_link": "#bluesky/stop/time",
                        },
                        "title": {
                            "_attributes": {
                                "NDAttrDescription": "sample name",
                                "NDAttrName": "SampleTitle",
                                "NDAttrSource": "91dcLAX:sampleTitle",
                                "NDAttrSourceType": "NDAttrSourceEPICSPV",
                            },
                            "_link": "#bluesky/start/sample_name",
                        },
                        "program_name": "EPICS areaDetector",
                        "instrument": {
                            "_attributes": {"NX_Class": "NXInstrument",},
                            "name_1": "#bluesky/start/beamline_id",  # create a link
                            "name_2": {  # create a link with attributes?
                                "_attributes": {"NX_This": "NXThat"},
                                "_link": "#bluesky/start/beamline_id",
                            },
                            "aperture": {
                                "_attributes": {"NX_Class": "NXAperture",},
                                "vcenter": 1.0,
                                "vsize": 2.0,
                                "description": "USAXSslit",
                            },
                        },
                    },
                },
            },
            # more techniques ...
        ]
    }
}


def test_start_nexus_metadata(caplog, tmp_path):
    caplog.set_level(logging.DEBUG, logger="suitcase.nxsas")

    start_doc_md = {}
    start_doc_md.update(rsoxs_start_doc)
    start_doc_md.update(techniques_md)
    # componse_run will raise an exception if "time" or "uid" are in the metadata
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
    documents.append(("start", start_doc))
    stop_doc = compose_stop()

    documents.append(("stop", stop_doc))
    artifacts = export(documents, tmp_path)

    assert len(artifacts["stream_data"]) == 1

    output_filepath = artifacts["stream_data"][0]
    assert output_filepath.exists()

    with h5py.File(output_filepath, "r") as h5f:
        assert "bluesky" in h5f
        print(list(h5f["bluesky"]))
        assert "start" in h5f["bluesky"]
        assert len(h5f["bluesky"]["start"]) == 42
        assert len(h5f["bluesky"].attrs) == 0
        assert all(h5f["bluesky"]["start"]["detectors"][()] == ["Synced", "en_energy"])
        assert all(
            h5f["bluesky"]["start"]["motors"][()]
            == ["WAXS Exposure", "SAXS Exposure", "en"]
        )
        assert h5f["bluesky"]["start"]["num_intervals"][()] == 127
        assert h5f["bluesky"]["start"]["num_points"][()] == 128
        assert h5f["bluesky"]["start"]["plan_name"][()] == "full_carbon_scan_nd"
        assert h5f["bluesky"]["start"]["plan_type"][()] == "generator"
        assert h5f["bluesky"]["start"]["scan_id"][()] == 6852
        assert h5f["bluesky"]["start"]["time"][()] == start_doc["time"]
        assert h5f["bluesky"]["start"]["uid"][()] == start_doc["uid"]

        assert len(h5f["bluesky"]) == 4
        assert "hints" in h5f["bluesky"]["start"]
        assert "dimensions" in h5f["bluesky"]["start"]["hints"]
        # the "dimensions" attribute has been jsonified because it is complicated
        # assert (
        #     h5f["bluesky"]["hints"].attrs["dimensions"]
        #     == '[[["random_walk:dt"], "primary"]]'
        # )
        # assert json.loads(h5f["bluesky"]["hints"].attrs["dimensions"]) == [
        #     [["random_walk:dt"], "primary"]
        # ]

        assert "md" in h5f["bluesky"]["start"]

        assert "plan_args" in h5f["bluesky"]["start"]
        assert "detectors" in h5f["bluesky"]["start"]["plan_args"]
        # assert h5f["bluesky"]["start"]["plan_args"][()] == start_doc["plan_args"]


def test_descriptor_nexus_metadata(caplog, tmp_path):
    caplog.set_level(logging.DEBUG, logger="suitcase.nxsas")

    start_doc_md = {}
    start_doc_md.update(rsoxs_start_doc)
    start_doc_md.update(techniques_md)
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

    documents.append(("start", start_doc))

    descriptor_doc_md = dict()
    descriptor_doc_md.update(rsoxs_descriptor_en_doc)
    # compose_descriptor will raise an exception if "run_start" is in the metadata
    descriptor_doc_md.pop("run_start")
    descriptor_doc, _, _ = compose_descriptor(**descriptor_doc_md)
    documents.append(("descriptor", descriptor_doc))

    stop_doc = compose_stop()
    documents.append(("stop", stop_doc))
    artifacts = export(documents, tmp_path)

    assert len(artifacts["stream_data"]) == 1

    output_filepath = artifacts["stream_data"][0]
    assert output_filepath.exists()

    with h5py.File(output_filepath, "r") as h5f:
        assert "bluesky" in h5f
        print(list(h5f["bluesky"]))

        assert "primary" in h5f["bluesky"]["descriptors"]
        assert "data_keys" in h5f["bluesky"]["descriptors"]["primary"]
        assert "en_energy" in h5f["bluesky"]["descriptors"]["primary"]["data_keys"]


def test_event_page_nexus_metadata(tmp_path):
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

    documents.append(("start", start_doc))

    descriptor_doc_md = dict()
    descriptor_doc_md.update(rsoxs_descriptor_en_doc)
    # compose_descriptor will raise an exception if "run_start" is in the metadata
    descriptor_doc_md.pop("run_start")
    descriptor_doc, compose_event, compose_event_page = compose_descriptor(
        **descriptor_doc_md
    )
    documents.append(("descriptor", descriptor_doc))

    event_md = dict()
    event_md.update(rsoxs_event_page_en_doc)
    # event_md["seq_num"] = [1]
    # the descriptor uid will interfere with compose_event
    event_md.pop("descriptor")
    event_doc = compose_event(**event_md)
    documents.append(("event", event_doc))

    stop_doc = compose_stop()
    documents.append(("stop", stop_doc))
    artifacts = export(documents, tmp_path)

    assert len(artifacts["stream_data"]) == 1

    output_filepath = artifacts["stream_data"][0]
    assert output_filepath.exists()

    with h5py.File(output_filepath, "r") as h5f:
        assert "bluesky" in h5f

        assert "primary" in h5f["bluesky"]["events"]

        assert "en_energy" in h5f["bluesky"]["events"]["primary"]["data"]
        assert h5f["bluesky"]["events"]["primary"]["data"]["en_energy"].shape == (1,)
        assert h5f["bluesky"]["events"]["primary"]["data"]["en_energy"][()] == [
            270.0012299
        ]

        # now test the NeXus structure
        assert "entry" in h5f
        assert len(h5f["entry"].attrs) == 2
        assert h5f["entry"].attrs["NX_Class"] == "NXEntry"
        assert h5f["entry"].attrs["default"] == "data"

        assert "end_time" in h5f["entry"]
        assert isinstance(h5f["entry"]["end_time"], h5py.Dataset)
        print(f"end_time: {h5f['entry']['end_time']}")
        assert h5f["entry"]["end_time"][()] == stop_doc["time"]
        assert len(h5f["entry"]["end_time"].attrs) == 4
