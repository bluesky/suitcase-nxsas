import json

import h5py
import numpy as np

import event_model

from suitcase.nxsas import export

start_doc_md = {
    "detectors": ["random_walk:x"],
    "hints": {"dimensions": [(["random_walk:dt"], "primary")]},
    "motors": ("random_walk:dt",),  # must use list not tuple?
    "num_intervals": 2,
    "num_points": 3,
    "plan_args": {
        "args": [
            "EpicsSignal(read_pv='random_walk:dt', "
            "name='random_walk:dt', "
            "value=1.0, "
            "timestamp=1550070001.828528, "
            "auto_monitor=False, "
            "string=False, "
            "write_pv='random_walk:dt', "
            "limits=False, "
            "put_complete=False)",
            -1,
            1,
        ],
        "detectors": [
            "EpicsSignal(read_pv='random_walk:x', "
            "name='random_walk:x', "
            "value=1.61472277847348, "
            "timestamp=1550070000.807677, "
            "auto_monitor=False, "
            "string=False, "
            "write_pv='random_walk:x', "
            "limits=False, "
            "put_complete=False)"
        ],
        "num": 3,
        "per_step": "None",
    },
    "plan_name": "scan",
    "plan_pattern": "inner_product",
    "plan_pattern_args": {
        "args": [
            "EpicsSignal(read_pv='random_walk:dt', "
            "name='random_walk:dt', "
            "value=1.0, "
            "timestamp=1550070001.828528, "
            "auto_monitor=False, "
            "string=False, "
            "write_pv='random_walk:dt', "
            "limits=False, "
            "put_complete=False)",
            -1,
            1,
        ],
        "num": 3,
    },
    "plan_pattern_module": "bluesky.plan_patterns",
    "plan_type": "generator",
    "scan_id": 2,
    # "time": 1550070004.9850419,
    # "uid": "ba1f9076-7925-4af8-916e-0e1eaa1b3c47",
    "md": {
        "techniques": [
            # SAXS technique
            {
                "version": 1,
                "technique": "SAXS",
                "nxsas": {
                    "_attributes": {"NX_Class": "NXEntry", "default": "data"},
                    "instrument": {
                        "_attributes": {"NX_Class": "NXInstrument",},
                        "name": "#bluesky/start@beamline_id",
                        "aperture": {
                            "_attributes": {"NX_Class": "NXAperture",},
                            "vcenter": 1.0,
                            "vsize": 2.0,
                            "description": "USAXSslit",
                        },
                    },
                },
            },
        ]
    },
}


def test_start_nexus_metadata(tmp_path):
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
    print(start_doc)
    documents.append(("start", start_doc))
    stop_doc = compose_stop()
    print(stop_doc)
    documents.append(("stop", stop_doc))
    artifacts = export(documents, tmp_path)

    assert len(artifacts["stream_data"]) == 1
    print(artifacts)

    output_filepath = artifacts["stream_data"][0]
    assert output_filepath.exists()

    with h5py.File(output_filepath, "r") as h5f:
        assert "bluesky" in h5f
        print(list(h5f["bluesky"]))
        assert len(h5f["bluesky"]["start"]) == 15
        assert h5f["bluesky"]["start"]["detectors"][()] == ["random_walk:x"]
        assert h5f["bluesky"]["start"]["motors"][()] == ["random_walk:dt"]
        assert h5f["bluesky"]["start"]["num_intervals"][()] == 2
        assert h5f["bluesky"]["start"]["num_points"][()] == 3
        assert h5f["bluesky"]["start"]["plan_name"][()] == "scan"
        assert h5f["bluesky"]["start"]["plan_pattern"][()] == "inner_product"
        assert (
            h5f["bluesky"]["start"]["plan_pattern_module"][()]
            == "bluesky.plan_patterns"
        )
        assert h5f["bluesky"]["start"]["plan_type"][()] == "generator"
        assert h5f["bluesky"]["start"]["scan_id"][()] == 2
        assert h5f["bluesky"]["start"]["time"][()] == start_doc["time"]
        assert h5f["bluesky"]["start"]["uid"][()] == start_doc["uid"]

        assert len(h5f["bluesky"]) == 4
        assert "hints" in h5f["bluesky"]["start"]
        assert "dimensions" in h5f["bluesky"]["start"]["hints"]
        # the "dimensions" attribute has been jsonified because it is complicated
        assert (
            h5f["bluesky"]["start"]["hints"]["dimensions"][()]
            == '[[["random_walk:dt"], "primary"]]'
        )
        assert json.loads(h5f["bluesky"]["start"]["hints"]["dimensions"][()]) == [
            [["random_walk:dt"], "primary"]
        ]

        assert "md" in h5f["bluesky"]["start"]

        assert "plan_args" in h5f["bluesky"]["start"]
        assert h5f["bluesky"]["start"]["plan_args"]["args"][()] == json.dumps(
            start_doc["plan_args"]["args"]
        )

        assert "plan_pattern_args" in h5f["bluesky"]["start"]


# 'event descriptor' document
descriptor_doc_md = {
    "configuration": {
        "random_walk:dt": {
            "data": {"random_walk:dt": -1.0},
            "data_keys": {
                "random_walk:dt": {
                    "dtype": "number",
                    "lower_ctrl_limit": 0.0,
                    "precision": 0,
                    "shape": [],
                    "source": "PV:random_walk:dt",
                    "units": "",
                    "upper_ctrl_limit": 0.0,
                }
            },
            "timestamps": {"random_walk:dt": 1550070004.994477},
        },
        "random_walk:x": {
            "data": {"random_walk:x": 1.9221013521832928},
            "data_keys": {
                "random_walk:x": {
                    "dtype": "number",
                    "lower_ctrl_limit": 0.0,
                    "precision": 0,
                    "shape": [],
                    "source": "PV:random_walk:x",
                    "units": "",
                    "upper_ctrl_limit": 0.0,
                }
            },
            "timestamps": {"random_walk:x": 1550070004.812525},
        },
    },
    "data_keys": {
        "random_walk:dt": {
            "dtype": "number",
            "lower_ctrl_limit": 0.0,
            "object_name": "random_walk:dt",
            "precision": 0,
            "shape": [],
            "source": "PV:random_walk:dt",
            "units": "",
            "upper_ctrl_limit": 0.0,
        },
        "random_walk:x": {
            "dtype": "number",
            "lower_ctrl_limit": 0.0,
            "object_name": "random_walk:x",
            "precision": 0,
            "shape": [],
            "source": "PV:random_walk:x",
            "units": "",
            "upper_ctrl_limit": 0.0,
        },
    },
    "hints": {
        "random_walk:dt": {"fields": ["random_walk:dt"]},
        "random_walk:x": {"fields": ["random_walk:x"]},
    },
    "name": "primary",
    "object_keys": {
        "random_walk:dt": ["random_walk:dt"],
        "random_walk:x": ["random_walk:x"],
    },
}


def test_descriptor_nexus_metadata(tmp_path):
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

    print(start_doc)
    documents.append(("start", start_doc))
    descriptor_doc, _, _ = compose_descriptor(**descriptor_doc_md)
    documents.append(("descriptor", descriptor_doc))
    stop_doc = compose_stop()
    print(stop_doc)
    documents.append(("stop", stop_doc))
    artifacts = export(documents, tmp_path)

    assert len(artifacts["stream_data"]) == 1
    print(artifacts)

    output_filepath = artifacts["stream_data"][0]
    assert output_filepath.exists()

    with h5py.File(output_filepath, "r") as h5f:
        assert "bluesky" in h5f
        print(list(h5f["bluesky"]))

        assert "primary" in h5f["bluesky"]["descriptors"]
        print(list(h5f["bluesky"]["descriptors"]["primary"]))


event_page_md = {
    "data": {"random_walk:dt": [-1.0], "random_walk:x": [1.9221013521832928]},
    "filled": {},
    "seq_num": [1],
    "timestamps": {
        "random_walk:dt": [1550070004.994477],
        "random_walk:x": [1550070004.812525],
    },
}


def test_event_page_nexus_metadata(tmp_path):
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

    print(start_doc)
    documents.append(("start", start_doc))
    descriptor_doc, compose_event, compose_event_page = compose_descriptor(
        **descriptor_doc_md
    )
    documents.append(("descriptor", descriptor_doc))

    event_page = compose_event_page(**event_page_md)
    documents.append(("event_page", event_page))

    stop_doc = compose_stop()
    print(stop_doc)
    documents.append(("stop", stop_doc))
    artifacts = export(documents, tmp_path)

    assert len(artifacts["stream_data"]) == 1
    print(artifacts)

    output_filepath = artifacts["stream_data"][0]
    assert output_filepath.exists()

    with h5py.File(output_filepath, "r") as h5f:
        assert "bluesky" in h5f
        print(list(h5f["bluesky"]))

        assert "primary" in h5f["bluesky"]["events"]

        assert "random_walk:dt" in h5f["bluesky"]["events"]["primary"]["data"]
        assert h5f["bluesky"]["events"]["primary"]["data"]["random_walk:dt"].shape == (
            1,
        )
        assert h5f["bluesky"]["events"]["primary"]["data"]["random_walk:dt"][
            ()
        ] == np.array([-1.0])

        assert "random_walk:x" in h5f["bluesky"]["events"]["primary"]["data"]
        assert h5f["bluesky"]["events"]["primary"]["data"]["random_walk:x"].shape == (
            1,
        )
        assert h5f["bluesky"]["events"]["primary"]["data"]["random_walk:x"][
            ()
        ] == np.array([1.9221013521832928])
