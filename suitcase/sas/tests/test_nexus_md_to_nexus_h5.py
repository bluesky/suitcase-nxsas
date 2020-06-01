from pathlib import Path

import h5py

from suitcase.sas import _copy_nexus_md_to_nexus_h5


def test_group_with_attributes(tmp_path):
    md = {"entry": {"_attributes": {"NX_Class": "NXEntry", "default": "data"}}}

    filepath = tmp_path / Path("test.h5")
    with h5py.File(filepath, "w") as f:
        _copy_nexus_md_to_nexus_h5(nexus_md=md, h5_group=f)

    with h5py.File(filepath, "r") as f:
        # expect this structure:
        #    /<group "entry">
        #       <attr "NX_Class": "NXEntry">
        #       <attr "default": "data">
        assert len(f) == 1
        assert "entry" in f
        assert len(f["entry"].attrs) == 2
        assert f["entry"].attrs["NX_Class"] == "NXEntry"
        assert f["entry"].attrs["default"] == "data"


def test_group_with_dataset(tmp_path):
    md = {
        "entry": {
            "_attributes": {"NX_Class": "NXEntry", "default": "data"},
            "program_name": "EPICS areaDetector",
        }
    }

    filepath = tmp_path / Path("test.h5")
    with h5py.File(filepath, "w") as f:
        _copy_nexus_md_to_nexus_h5(nexus_md=md, h5_group=f)

    with h5py.File(filepath, "r") as f:
        # expect this structure:
        #    /<group "entry">
        #       <attr "NX_Class": "NXEntry">
        #       <attr "default": "data">
        #       <dataset "program_name": "EPICS areaDetector">
        assert len(f) == 1
        assert "entry" in f
        assert len(f["entry"].attrs) == 2
        assert f["entry"].attrs["NX_Class"] == "NXEntry"
        assert f["entry"].attrs["default"] == "data"

        assert "program_name" in f["entry"]
        assert isinstance(f["entry"]["program_name"], h5py.Dataset)
        assert f["entry"]["program_name"][()] == "EPICS areaDetector"


def test_group_with_dataset_link(tmp_path):
    md = {
        "entry": {
            "_attributes": {"NX_Class": "NXEntry", "default": "data"},
            "GUPNumber": "#bluesky/start/gup_number",
        }
    }
    filepath = tmp_path / Path("test.h5")
    with h5py.File(filepath, "w") as f:
        # create a target dataset for #bluesky/start/gup_number
        f.create_group("bluesky").create_group("start").create_dataset(
            name="gup_number", data=1
        )
        _copy_nexus_md_to_nexus_h5(nexus_md=md, h5_group=f)

    with h5py.File(filepath, "r") as f:
        # expect this structure:
        #    /<group "bluesky">
        #        <group "start">
        #            <dataset "gup_number": 1>
        #    /<group "entry">
        #       <attr "NX_Class": "NXEntry">
        #       <attr "default": "data">
        #       <link "GUPNumber" <dataset bluesky/start/gup_number>>
        assert len(f) == 2

        assert "bluesky" in f
        assert "start" in f["bluesky"]
        assert "gup_number" in f["bluesky"]["start"]
        assert f["bluesky"]["start"]["gup_number"][()] == 1

        assert "entry" in f
        assert len(f["entry"].attrs) == 2
        assert f["entry"].attrs["NX_Class"] == "NXEntry"
        assert f["entry"].attrs["default"] == "data"

        assert "GUPNumber" in f["entry"]
        assert isinstance(f["entry"]["GUPNumber"], h5py.Dataset)
        assert f["entry"]["GUPNumber"][()] == 1

        assert f["entry"]["GUPNumber"] == f["bluesky"]["start"]["gup_number"]


def test_dataset_link_with_attributes(tmp_path):
    md = {
        "entry": {
            "_attributes": {"NX_Class": "NXEntry", "default": "data"},
            "GUPNumber": {
                "_attributes": {
                    "NDAttrDescription": "GUP proposal number",
                    "NDAttrName": "GUPNumber",
                    "NDAttrSource": "91dcLAX:GUPNumber",
                    "NDAttrSourceType": "NDAttrSourceEPICSPV",
                },
                "_link": "#bluesky/start/gup_number",
            },
        }
    }
    filepath = tmp_path / Path("test.h5")
    with h5py.File(filepath, "w") as f:
        # create a target dataset for #bluesky/start/gup_number
        f.create_group("bluesky").create_group("start").create_dataset(
            name="gup_number", data=1
        )
        _copy_nexus_md_to_nexus_h5(nexus_md=md, h5_group=f)

    with h5py.File(filepath, "r") as f:
        # expect this structure:
        #    /<group "bluesky">
        #        <group "start">
        #            <dataset "gup_number": 1>
        #    /<group "entry">
        #       <attr "NX_Class": "NXEntry">
        #       <attr "default": "data">
        #       <link "GUPNumber" <dataset bluesky/start/gup_number>>
        #           <attr "NDAttrDescription": "GUP proposal number">
        #           <attr "NDAttrName": "GUPNumber">
        #           <attr "NDAttrSource": "91dcLAX:GUPNumber">
        #           <attr "NDAttrSourceType": "NDAttrSourceEPICSPV">
        assert len(f) == 2

        assert "bluesky" in f
        assert "start" in f["bluesky"]
        assert "gup_number" in f["bluesky"]["start"]
        assert f["bluesky"]["start"]["gup_number"][()] == 1

        assert "entry" in f
        assert len(f["entry"].attrs) == 2
        assert f["entry"].attrs["NX_Class"] == "NXEntry"
        assert f["entry"].attrs["default"] == "data"

        assert "GUPNumber" in f["entry"]
        assert isinstance(f["entry"]["GUPNumber"], h5py.Dataset)
        assert f["entry"]["GUPNumber"][()] == 1

        assert f["entry"]["GUPNumber"] == f["bluesky"]["start"]["gup_number"]

        assert len(f["entry"]["GUPNumber"].attrs) == 4
        assert (
            f["entry"]["GUPNumber"].attrs["NDAttrDescription"] == "GUP proposal number"
        )
        assert f["entry"]["GUPNumber"].attrs["NDAttrName"] == "GUPNumber"
        assert f["entry"]["GUPNumber"].attrs["NDAttrSource"] == "91dcLAX:GUPNumber"
        assert (
            f["entry"]["GUPNumber"].attrs["NDAttrSourceType"] == "NDAttrSourceEPICSPV"
        )


def test_group_with_subgroup(tmp_path):
    md = {
        "entry": {
            "_attributes": {"NX_Class": "NXEntry", "default": "data"},
            "instrument": {
                "_attributes": {"NX_Class": "NXInstrument",},
                "name_1": "#bluesky/start/beamline_id",
                "name_2": {
                    "_attributes": {"NX_This": "NXThat"},
                    "_link": "#bluesky/start/beamline_id",
                },
            },
        },
    }
    filepath = tmp_path / Path("test.h5")
    with h5py.File(filepath, "w") as f:
        # create a target dataset for #bluesky/start/gup_number
        f.create_group("bluesky").create_group("start").create_dataset(
            name="beamline_id", data="RSOXS"
        )
        _copy_nexus_md_to_nexus_h5(nexus_md=md, h5_group=f)

    with h5py.File(filepath, "r") as f:
        # expect this structure:
        #    /<group "bluesky">
        #        <group "start">
        #            <dataset "gup_number": 1>
        #    /<group "entry">
        #       <attr "NX_Class": "NXEntry">
        #       <attr "default": "data">
        #       <dataset "name_1": "">
        #       <link "GUPNumber" <dataset bluesky/start/gup_number>>
        #           <attr "NDAttrDescription": "GUP proposal number">
        #           <attr "NDAttrName": "GUPNumber">
        #           <attr "NDAttrSource": "91dcLAX:GUPNumber">
        #           <attr "NDAttrSourceType": "NDAttrSourceEPICSPV">
        assert len(f) == 2



def test(tmp_path):
    md = {
        "techniques": [
            # SAXS technique
            {
                "version": 1,
                "technique": "SAXS",
                "nxsas": {
                    "entry": {
                        "_attributes": {"NX_Class": "NXEntry", "default": "data"},
                        "instrument": {
                            "_attributes": {"NX_Class": "NXInstrument",},
                            "name": "#bluesky/start/beamline_id",
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

    filepath = tmp_path / Path("test.h5")
    with h5py.File(filepath, "w") as f:
        f.create_group("bluesky").create_group("start").create_dataset(
            name="beamline_id", data="SST-1 RSoXS"
        )

        _copy_nexus_md_to_nexus_h5(nexus_md=md["techniques"][0]["nxsas"], h5_group=f)

    with h5py.File(filepath, "r") as f:
        print(list(f))
        assert "entry" in f
        print(list(f["entry"]))
        entry_h5_group = f["entry"]

        assert len(entry_h5_group.attrs) == 2
        assert entry_h5_group.attrs["NX_Class"] == "NXEntry"
        assert entry_h5_group.attrs["default"] == "data"

        assert "instrument" in entry_h5_group
        assert len(entry_h5_group["instrument"].attrs) == 1
        assert entry_h5_group["instrument"].attrs["NX_Class"] == "NXInstrument"
        # what does [()] mean?
        print(entry_h5_group["instrument"]["name"])
        assert entry_h5_group["instrument"]["name"][()] == "SST-1 RSoXS"

        assert "aperture" in entry_h5_group["instrument"]
        assert len(entry_h5_group["instrument"]["aperture"].attrs) == 1
        assert (
            entry_h5_group["instrument"]["aperture"].attrs["NX_Class"] == "NXAperture"
        )
        assert entry_h5_group["instrument"]["aperture"]["vcenter"][()] == 1.0
        assert entry_h5_group["instrument"]["aperture"]["vsize"][()] == 2.0
        assert (
            entry_h5_group["instrument"]["aperture"]["description"][()] == "USAXSslit"
        )
