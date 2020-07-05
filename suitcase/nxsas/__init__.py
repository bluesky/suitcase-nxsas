# Suitcase subpackages should follow strict naming and interface conventions.
# The public API must include Serializer and should include export if it is
# intended to be user-facing. They should accept the parameters sketched here,
# but may also accept additional required or optional keyword arguments, as
# needed.
import copy
import logging
from pathlib import Path

import h5py
import numpy as np

import event_model

from .utils import (
    _copy_nexus_md_to_nexus_h5,
    _copy_metadata_to_h5_datasets,
)

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions


def export(gen, directory, file_prefix="{uid}-", **kwargs):
    """
    Export a stream of documents to nxsas.

    .. note::

        This can alternatively be used to write data to generic buffers rather
        than creating files on disk. See the documentation for the
        ``directory`` parameter below.

    Parameters
    ----------
    gen : generator
        expected to yield ``(name, document)`` pairs

    directory : string, Path or Manager.
        For basic uses, this should be the path to the output directory given
        as a string or Path object. Use an empty string ``''`` to place files
        in the current working directory.

    file_prefix : str, optional
        The first part of the filename of the generated output files. This
        string may include templates as in ``{proposal_id}-{sample_name}-``,
        which are populated from the RunStart document. The default value is
        ``{uid}-`` which is guaranteed to be present and unique. A more
        descriptive value depends on the application and is therefore left to
        the user.

    **kwargs : kwargs
        Keyword arugments to be passed through to the underlying I/O library.

    Returns
    -------
    artifacts : dict
        dict mapping the 'labels' to lists of file names (or, in general,
        whatever resources are produced by the Manager)

    Examples
    --------

    Generate files with unique-identifier names in the current directory.

    >>> export(gen, '')

    Generate files with more readable metadata in the file names.

    >>> export(gen, '', '{plan_name}-{motors}-')

    Include the experiment's start time formatted as YYYY-MM-DD_HH-MM.

    >>> export(gen, '', '{time:%Y-%m-%d_%H:%M}-')

    Place the files in a different directory, such as on a mounted USB stick.

    >>> export(gen, '/path/to/my_usb_stick')
    """
    with Serializer(directory, file_prefix, **kwargs) as serializer:
        for item in gen:
            serializer(*item)

    return serializer.artifacts


class Serializer(event_model.SingleRunDocumentRouter):
    """
    Serialize a stream of documents to nxsas.

    .. note::

        This can alternatively be used to write data to generic buffers rather
        than creating files on disk. See the documentation for the
        ``directory`` parameter below.

    Parameters
    ----------
    directory : string, Path
        The path to the output directory given as a string or Path object.
        Use an empty string ``''`` to place files in the current working directory.

    file_prefix : str, optional
        The first part of the filename of the generated output files. This
        string may include templates as in ``{proposal_id}-{sample_name}-``,
        which are populated from the RunStart document. The default value is
        ``{uid}-`` which is guaranteed to be present and unique. A more
        descriptive value depends on the application and is therefore left to
        the user.

    **kwargs : kwargs
        Keyword arguments to be passed through to the underlying I/O library.

    Attributes
    ----------
    artifacts
        dict mapping the 'labels' to lists of file names (or, in general,
        whatever resources are produced by the Manager)
    """

    def __init__(self, directory, file_prefix="{uid}-", **kwargs):
        super().__init__()
        self.log = logging.getLogger("suitcase.nxsas")

        if not isinstance(directory, Path):
            directory = Path(directory)
        self.directory = directory
        self._file_prefix = file_prefix
        self._kwargs = kwargs

        self._templated_file_prefix = None  # set when we get a 'start' document
        self.output_filepath = None
        self._h5_output_file = None

        self.bluesky_h5_group_name = "bluesky"

    @property
    def artifacts(self):
        # The 'artifacts' are the manager's way to exposing to the user a
        # way to get at the resources that were created. For
        # `MultiFileManager`, the artifacts are filenames.  For
        # `MemoryBuffersManager`, the artifacts are the buffer objects
        # themselves. The Serializer, in turn, exposes that to the user here.
        #
        # This must be a property, not a plain attribute, because the
        # manager's `artifacts` attribute is also a property, and we must
        # access it anew each time to be sure to get the latest contents.
        if self.output_filepath is None:
            raise Exception("No artifacts have been created yet.")

        return {"stream_data": [self.output_filepath]}

    def close(self):
        """
        Close all of the resources (e.g. files) allocated.
        """
        self._h5_output_file.close()

    # These methods enable the Serializer to be used as a context manager:
    #
    # with Serializer(...) as serializer:
    #     ...
    #
    # which always calls close() on exit from the with block.

    def __enter__(self):
        return self

    def __exit__(self, *exception_details):
        self.close()

    def start(self, start_doc):
        """
        Determine the name for the HDF5 output file and open it.
        Create a top-level group for bluesky data.
        Create "start", "descriptors", "events", and "stop" groups.
        Copy start document data to the H5 start group.

        The top level groups look like this:
        /
            bluesky/  <-- found in bluesky_h5_group_name
                start/
                descriptors/
                events/
                stop/

        Parameters
        ==========
        start_doc: dict
            RunStart document
        """
        super().start(start_doc)

        # Fill in the file_prefix with the contents of the RunStart document.
        # As in, '{uid}' -> 'c1790369-e4b2-46c7-a294-7abfa239691a'
        # or 'my-data-from-{plan-name}' -> 'my-data-from-scan'
        self.log.info("new run detected uid=%s", start_doc["uid"])
        self._templated_file_prefix = self._file_prefix.format(**start_doc)
        self.filename = Path(self._templated_file_prefix + ".h5")

        self.log.info("creating file %s in directory %s", self.filename, self.directory)
        self.output_filepath = self.directory / self.filename
        self._h5_output_file = h5py.File(self.output_filepath, "w")

        # create a top-level group to hold bluesky document information
        h5_bluesky_group = self._h5_output_file.create_group(self.bluesky_h5_group_name)

        # create the start group and copy the start document to it
        h5_start_group = h5_bluesky_group.create_group("start")
        _copy_metadata_to_h5_datasets(a_mapping=start_doc, h5_group=h5_start_group)

        # create groups for descriptors, events, and stop documents
        h5_bluesky_group.create_group("descriptors")
        h5_bluesky_group.create_group("events")
        h5_bluesky_group.create_group("stop")

    def descriptor(self, descriptor_doc):
        """
        Create a HDF5 group under the "descriptors" group using the stream name.
        Copy the descriptor document to the new group.
        Create a HDF5 group under the "events" group with the stream name.
        Create two HDF5 groups under the "events/<stream name>" group called "data" and "timestamps".

        /
            bluesky/
                start/
                    ...start document metadata...
                descriptors/
                    primary/                                 <-- for example, the "primary" stream
                        ...descriptor document metadata...
                events/
                    primary/
                        data/
                        timestamps/
                stop/

        Parameters
        ==========
        descriptor_doc: dict
            EventDescriptor document
        """
        super().descriptor(descriptor_doc)

        h5_bluesky_group = self._h5_output_file[self.bluesky_h5_group_name]
        h5_bluesky_descriptors_group = h5_bluesky_group["descriptors"]

        # create a group for this descriptor, use the stream name
        # copy the descriptor document metadata to H5 datasets
        stream_name = descriptor_doc["name"]
        h5_descriptor_stream_group = h5_bluesky_descriptors_group.create_group(
            stream_name
        )
        _copy_metadata_to_h5_datasets(
            a_mapping=descriptor_doc, h5_group=h5_descriptor_stream_group
        )

        # create a group to hold datasets
        # each row of a dataset will be read from an event_page document
        h5_bluesky_group["events"].create_group(stream_name).create_group("data")

        # create a group to hold timestamps
        h5_bluesky_group["events"][stream_name].create_group("timestamps")

    def event_page(self, event_page_doc):
        """
        Create an HDF5 dataset for each data_key when the first EventPage in a stream arrives.
        Append data to each dataset for each data_key when subsequent EventPages arrive.

        /
            bluesky/
                start/
                    ...start document metadata...
                descriptors/
                    primary/
                        ...descriptor document metadata...
                events/
                    primary/
                        data/
                            data_key_1 dataset   <-- create data_key datasets when the first EventPage arrives
                                [1, 2, ...]
                            data_key_2 dataset
                                [[1, 2, ...],
                                 [4, 5, ...],
                                 ...]
                            data_key_3 dataset
                                [
                                    [
                                        [1, 2, ...],
                                        [4, 5, ...],
                                        ...
                                    ],
                                    [
                                        [9, 8, ...],
                                        [6, 5, ...],
                                        ...
                                    ],
                                    ...
                                ]
                            ...
                        timestamps/
                            data_key_1 dataset   <-- create timestamp datasets when the first EventPage arrives
                                [1593857592.87652, 1593857592.87652, ...]
                            data_key_2 dataset
                                [1593857592.87652, 1593857592.87652, ...]
                            data_key_2 dataset
                                [1593857592.87652, 1593857592.87652, ...]
                stop/

        Parameters
        ==========
        descriptor_doc: dict
            EventDescriptor document
        """
        super().event_page(event_page_doc)
        # There are other representations of Event data -- 'event' and
        # 'bulk_events' (deprecated). But that does not concern us because
        # DocumentRouter will convert these representations to 'event_page'
        # then route them through here.
        stream_name = self.get_stream_name(doc=event_page_doc)

        h5_descriptor_stream_group = self._h5_output_file[self.bluesky_h5_group_name][
            "descriptors"
        ][stream_name]
        h5_event_stream_group = self._h5_output_file[self.bluesky_h5_group_name][
            "events"
        ][stream_name]
        h5_event_stream_data_group = h5_event_stream_group["data"]
        h5_event_stream_data_timestamps_group = h5_event_stream_group["timestamps"]

        for ep_data_key, ep_data_list in event_page_doc["data"].items():
            if event_page_doc["filled"].get(ep_data_key, None) is False:
                raise ValueError(
                    f"data_key {ep_data_key} must be filled "
                    f" in stream/event/run: {stream_name}/{event_page_doc['uid']}/{self.get_start()['uid']}"
                )
            # Data in an event_page will *always* be inside a list because
            # an event_page contains data from one or more events.
            # ep_data_list will have only one element if the event page
            # contains data for exactly one event
            # for example:
            #    scalar per event                : ep_data_list = [1.0, 2.0, ...]
            #    one-dimensional array per event : ep_data_list = [[1, 2, ...], [3, 4, ...], ...]

            # convert the event page list of data to an array
            # this way there is a .shape to work with
            # TODO: could we get a list of things with different sizes and fail here?
            # TODO: this seems to be a deprecated use of np.asarray
            ep_data_array = np.asarray(ep_data_list)
            ep_data_timestamps_array = np.array(
                event_page_doc["timestamps"][ep_data_key]
            )

            self.log.debug(
                "event_page data_key %s has shape %s", ep_data_key, ep_data_array.shape
            )

            # retrieve information from the descriptor document
            # already stored in the HDF5 descriptor group
            h5_descriptor_stream_data_key_info = h5_descriptor_stream_group[
                "data_keys"
            ][ep_data_key]

            # is this the first event page document in the stream?
            if ep_data_key not in h5_event_stream_data_group:
                # this is the first event page document in the stream
                # prepare to create a HDF5 dataset for this data_key
                self.log.debug("dataset '%s' has not been created yet", ep_data_key)
                self.log.debug("event_page data: %s", ep_data_list)
                self.log.debug(
                    "descriptor for '%s': %s",
                    ep_data_key,
                    list(h5_descriptor_stream_data_key_info.values()),
                )

                # TODO: use a databroker transform instead
                if h5_descriptor_stream_data_key_info["dtype"][()] == "array":
                    self.check_and_correct_h5_descriptor_array_shape(
                        h5_descriptor_data_key_info=h5_descriptor_stream_data_key_info,
                        ep_data_key=ep_data_key,
                        ep_data_list=ep_data_list,
                    )

                h5_dataset_init_kwargs = {
                    "shape": ep_data_array.shape,
                    "name": ep_data_key,
                    "dtype": get_h5_dtype_from_descriptor_dtype(
                        descriptor_dtype=h5_descriptor_stream_data_key_info["dtype"][
                            ()
                        ],
                        ep_data_key=ep_data_key,
                        ep_data_list=ep_data_list,
                    ),
                    # chunks looks like shape with element 0 replaced by 1
                    # maxshape looks like shape with element 0 replaced by None
                    # for example:
                    #    shape     chunks    maxshape
                    #    (3, )     (1, )     (None, )
                    #    (3, 4)    (1, 4)    (None, 4)
                    #    (3, 4, 5) (1, 4, 5) (None, 4, 5)
                    "chunks": (1, *ep_data_array.shape[1:]),
                    "maxshape": (None, *ep_data_array.shape[1:]),
                }

                self.log.debug(
                    "creating dataset '%s' with kwargs %s",
                    ep_data_key,
                    h5_dataset_init_kwargs,
                )
                ds = h5_event_stream_data_group.create_dataset(
                    **h5_dataset_init_kwargs,
                )
                ds[:] = ep_data_array

                # also create a timestamps dataset for this data key
                h5_timestamps_dataset_init_kwargs = {
                    "shape": (ep_data_array.shape[0],),
                    "name": ep_data_key,
                    "dtype": "f8",
                    "chunks": (1,),
                    "maxshape": (None,),
                }

                ts = h5_event_stream_data_timestamps_group.create_dataset(
                    **h5_timestamps_dataset_init_kwargs,
                )
                ts[:] = ep_data_timestamps_array
            else:
                # the data and timestamps datasets already exist
                # append data to them

                ds = h5_event_stream_data_group[ep_data_key]
                ds.resize((ds.shape[0] + ep_data_array.shape[0], *ds.shape[1:]))
                ds[-(ep_data_array.shape[0]):] = ep_data_array

                ts = h5_event_stream_data_timestamps_group[ep_data_key]
                ts.resize(
                    (ts.shape[0] + ep_data_timestamps_array.shape[0], *ts.shape[1:],)
                )
                ts[-(ep_data_timestamps_array.shape[0]):] = ep_data_timestamps_array

    def stop(self, doc):
        super().stop(doc)

        _copy_metadata_to_h5_datasets(
            a_mapping=doc,
            h5_group=self._h5_output_file[self.bluesky_h5_group_name]["stop"],
        )

        # all bluesky documents have been serialized
        # now is the time to create the NeXuS structure
        # parse the "techniques" section of the start document
        start_doc = self.get_start()

        if "md" in start_doc and "techniques" in start_doc["md"]:
            techniques_md = copy.deepcopy(start_doc["md"]["techniques"])
            for technique_info in techniques_md:
                technique = technique_info["technique"]
                # "version" is mandatory
                technique_schema_version = technique_info["version"]
                self.log.info("technique: %s", technique)
                self.log.info("technique version: %s", technique_schema_version)

                _copy_nexus_md_to_nexus_h5(
                    nexus_md=technique_info["nxsas"], h5_group_or_dataset=self._h5_output_file
                )

        self.log.info("finished writing file %s", self.filename)

        self.close()

    def check_and_correct_h5_descriptor_array_shape(
        self, h5_descriptor_data_key_info, ep_data_key, ep_data_list
    ):
        # check for disagreement between the shape specified by the descriptor
        # and the shape of the filled event_page array
        # ep_data_info["shape"] looks like [b, a, 0] but should be [0, a, b]
        # this is a disagreement between databroker and AreaDetector

        # in the case of disagreement the shape in the descriptor might look like [1024 1026 0]
        # and the shape of the filled event_page array looks like (1026, 1024)
        shape_in_descriptor = tuple(h5_descriptor_data_key_info["shape"][()])
        shape_in_event_page = ep_data_list[0].shape
        self.log.debug(
            "data key %s: descriptor shape: %s event_page shape: %s",
            ep_data_key,
            shape_in_descriptor,
            shape_in_event_page,
        )
        if shape_in_descriptor[:2] == shape_in_event_page:
            # descriptor and event_page array shapes are equivalent
            # no correction necessary
            pass
        elif tuple(reversed(shape_in_descriptor))[1:] == shape_in_event_page:
            # for example,
            #     shape_in_descriptor = (1024, 1026, 0)
            #     shape_in_event_page = (1026, 1024)
            # will evaluate True:
            #     tuple(reverse((1024, 1026, 0))[1:] == (1026, 1024)
            # we need to correct the shape copied from the descriptor document
            self.log.warning(
                "reversing shape %s of data_key %s", shape_in_descriptor, ep_data_key,
            )
            # update the h5 descriptor shape
            h5_descriptor_data_key_info["shape"][()] = tuple(
                reversed(shape_in_descriptor)
            )
        else:
            raise ValueError(
                f"descriptor and event_page array shapes for data_key {ep_data_key} can not be reconciled"
            )


_descriptor_dtype_to_h5_dtype = {
    "string": h5py.string_dtype(),
    "number": "f8",
    "integer": "i4",
}


def get_h5_dtype_from_descriptor_dtype(descriptor_dtype, ep_data_key, ep_data_list):
    if descriptor_dtype == "array":
        # get the dtype from the first array in ep_data_list
        h5_dtype = ep_data_list[0].dtype
    elif descriptor_dtype in _descriptor_dtype_to_h5_dtype:
        h5_dtype = _descriptor_dtype_to_h5_dtype[descriptor_dtype]
    else:
        raise ValueError(
            f"Unknown descriptor dtype '{descriptor_dtype}' for data_key '{ep_data_key}'"
        )

    return h5_dtype


def get_h5_dataset_shape_from_descriptor_shape(
    descriptor_shape, ep_data_key, ep_data_list
):
    """
    Return the initial shape of the h5 dataset corresponding to the specified data_key.
    Initially the dataset will have length 0. It will be resized when data is appended.

    The descriptor shape will be either an empty list or a list of integers.

    The return will be a sequence of integers beginning with 0. For example:
        descriptor shape [] -> h5 dataset shape (1, )
        descriptor shape [0, 100, 100] -> h5 dataset (1, 100, 100)

    Parameters
    ----------
    descriptor_shape: Sequence of int, possibly empty
        the "shape" field for ep_data_key taken from a descriptor document
    ep_data_key: str
        bluesky data_key
    ep_data_list: list
        event_page list of data values: int, str, float, or array for ep_data_key

    Returns
    -------
    tuple, the initial shape of the h5 dataset corresponding to ep_data_key, the first element of which
    will be 0 to indicate the dataset should be created empty
    """
    print(f"descriptor_shape: {descriptor_shape}")
    if not isinstance(descriptor_shape, list):
        raise ValueError(
            f"descriptor_shape must be a list, but is a {type(descriptor_shape)}"
        )
    elif len(descriptor_shape) == 0:
        # descriptor_shape is []
        return (1,)
    else:
        if descriptor_shape[0] != 0:
            raise ValueError(
                f"descriptor shape for data_key {ep_data_key} should start with 0 but is {descriptor_shape}"
            )
        return (1, *descriptor_shape[1:])
