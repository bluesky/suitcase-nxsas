# Suitcase subpackages should follow strict naming and interface conventions.
# The public API must include Serializer and should include export if it is
# intended to be user-facing. They should accept the parameters sketched here,
# but may also accept additional required or optional keyword arguments, as
# needed.
import copy
import logging
from pathlib import Path

import h5py

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
        self.log = logging.Logger("suitcase.nxsas", level=logging.DEBUG)
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

    # Each of the methods below corresponds to a document type. As
    # documents flow in through Serializer.__call__, the DocumentRouter base
    # class will forward them to the method with the name corresponding to
    # the document's type: RunStart documents go to the 'start' method,
    # etc.
    def start(self, start_doc):
        super().start(start_doc)
        # Fill in the file_prefix with the contents of the RunStart document.
        # As in, '{uid}' -> 'c1790369-e4b2-46c7-a294-7abfa239691a'
        # or 'my-data-from-{plan-name}' -> 'my-data-from-scan'
        self.log.info("new run detected uid=%s", start_doc["uid"])
        self._templated_file_prefix = self._file_prefix.format(**start_doc)
        self.filename = self._templated_file_prefix + ".h5"

        self.log.info("creating file %s in directory %s", self.filename, self.directory)

        self.output_filepath = self.directory / Path(self.filename)
        self._h5_output_file = h5py.File(self.output_filepath, "w")

        # create a top-level group to hold bluesky document information
        bluesky_group = self._h5_output_file.create_group(self.bluesky_h5_group_name)
        self.log.info("created bluesky group %s", bluesky_group)

        # copy start document metadata to H5 datasets
        start_group = bluesky_group.create_group("start")
        _copy_metadata_to_h5_datasets(a_mapping=start_doc, h5_group=start_group)

        # create a group for descriptors
        bluesky_group.create_group("descriptors")

        # create a group for event data
        bluesky_group.create_group("events")

    def descriptor(self, descriptor_doc):
        super().descriptor(descriptor_doc)

        h5_bluesky_group = self._h5_output_file[self.bluesky_h5_group_name]
        h5_bluesky_descriptors_group = h5_bluesky_group["descriptors"]

        stream_name = descriptor_doc["name"]

        # create a group for this descriptor, use the stream name
        # copy the descriptor document metadata to H5 datasets
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

    def event_page(self, even_page_doc):
        super().event_page(even_page_doc)
        # There are other representations of Event data -- 'event' and
        # 'bulk_events' (deprecated). But that does not concern us because
        # DocumentRouter will convert these representations to 'event_page'
        # then route them through here.
        stream_name = self.get_stream_name(doc=even_page_doc)

        h5_descriptor_stream_group = self._h5_output_file[self.bluesky_h5_group_name][
            "descriptors"
        ][stream_name]

        h5_event_stream_group = self._h5_output_file[self.bluesky_h5_group_name][
            "events"
        ][stream_name]
        h5_event_stream_data_group = h5_event_stream_group["data"]
        h5_stream_data_timestamps_group = h5_event_stream_group["timestamps"]
        for ep_data_key, ep_data_list in even_page_doc["data"].items():
            ep_data = ep_data_list[0]
            # create the h5 dataset for this data key
            # the first time we see the data key in an event page
            if ep_data_key not in h5_event_stream_data_group:
                self.log.debug("dataset '%s' has not been created yet", ep_data_key)
                self.log.debug("data: %s", ep_data)
                self.log.debug(
                    "descriptor dtype: %s",
                    h5_descriptor_stream_group["data_keys"][ep_data_key]["dtype"][()],
                )
                h5_dataset_init_kwargs = {
                    "name": ep_data_key,
                    "dtype": None,  # we will discover this later
                    "shape": (0,),  # we will resize each time we add data
                    "maxshape": (None,),
                    "chunks": (1,),
                }

                h5_descriptor_stream_data_key_info = h5_descriptor_stream_group[
                    "data_keys"
                ][ep_data_key]
                if h5_descriptor_stream_data_key_info["dtype"][()] == "string":
                    h5_dataset_init_kwargs["dtype"] = h5py.string_dtype()
                elif h5_descriptor_stream_data_key_info["dtype"][()] == "number":
                    h5_dataset_init_kwargs["dtype"] = "f8"
                elif h5_descriptor_stream_data_key_info["dtype"] == "integer":
                    h5_dataset_init_kwargs["dtype"] = "i4"
                elif h5_descriptor_stream_data_key_info["dtype"][()] == "array":
                    # ep_data is a numpy array

                    # the event_page "shape" information is everything needed
                    # to specify the h5 dataset shape, maxshape, and chunks

                    # check for disagreement between the shape specified by the descriptor
                    # and the shape of the filled event_page array
                    # ep_data_info["shape"] looks like [b, a, 0] but should be [0, a, b]
                    # this is a disagreement between databroker and AreaDetector

                    # in the case of disagreement the shape in descriptor might look like [1024 1026 0]
                    # and the shape of the filled event_page array looks like (1026, 1024)
                    shape_in_desc = h5_descriptor_stream_data_key_info["shape"][()]
                    shape_in_event_page = ep_data.shape
                    self.log.debug("shape in descriptor: %s", shape_in_desc)
                    self.log.debug("shape in event page: %s", shape_in_event_page)
                    self.log.debug(tuple(reversed(ep_data.shape)))
                    if all(shape_in_desc[:2] == ep_data.shape):
                        self.log.debug("array shapes are the same!")
                    elif all(shape_in_desc[:2] == tuple(reversed(ep_data.shape))):
                        self.log.warning("array shapes are reversed!")
                        self.log.warning(
                            "reversed(shape_in_desc): %s",
                            tuple(reversed(shape_in_desc)),
                        )
                        h5_descriptor_stream_data_key_info["shape"][()] = tuple(
                            reversed(shape_in_desc)
                        )
                    else:
                        raise Exception("shapes are different!")

                    h5_dataset_init_kwargs["dtype"] = ep_data.dtype
                    h5_dataset_init_kwargs["shape"] = tuple(
                        h5_descriptor_stream_data_key_info["shape"]
                    )
                    h5_dataset_init_kwargs["maxshape"] = (
                        None,
                        *h5_descriptor_stream_data_key_info["shape"][1:],
                    )
                    h5_dataset_init_kwargs["chunks"] = (
                        1,
                        *h5_descriptor_stream_data_key_info["shape"][1:],
                    )

                if isinstance(ep_data, (list, tuple)):

                    # information from the event_page is needed
                    # to specify the dataset shapes
                    ep_data_length = len(ep_data)
                    self.log.debug(
                        "event page '%s' is a list with length %d",
                        ep_data_key,
                        ep_data_length,
                    )
                    h5_dataset_init_kwargs.update(
                        {
                            "shape": (0, ep_data_length),
                            "maxshape": (None, ep_data_length),
                            "chunks": (1, ep_data_length),
                        }
                    )
                else:
                    # no additional information to collect
                    pass

                self.log.debug(
                    "creating dataset '%s' with kwargs %s",
                    ep_data_key,
                    h5_dataset_init_kwargs,
                )
                h5_event_stream_data_group.create_dataset(**h5_dataset_init_kwargs)

                # also create a timestamps dataset for this data key
                h5_timestamps_dataset_init_kwargs = h5_dataset_init_kwargs.copy()
                # the only change needed is setting dtype to f8
                h5_timestamps_dataset_init_kwargs["dtype"] = "f8"
                h5_stream_data_timestamps_group.create_dataset(
                    **h5_timestamps_dataset_init_kwargs
                )

            h5_data_array = h5_event_stream_data_group[ep_data_key]
            self.log.debug(
                "found event_page data_key %s in h5 group %s",
                ep_data_key,
                h5_event_stream_data_group,
            )
            self.log.debug("%s has len() %s", h5_data_array, h5_data_array.len())

            # insert the new data from this event page into
            # the corresponding h5 dataset
            h5_data_array.resize((h5_data_array.shape[0] + 1, *h5_data_array.shape[1:]))
            if len(h5_data_array.shape) == 1:
                h5_data_array[-1] = ep_data
            else:
                h5_data_array[-1, :] = ep_data

            # insert the new timestamps
            h5_data_timestamps_array = h5_stream_data_timestamps_group[ep_data_key]
            h5_data_timestamps_array.resize(
                (
                    h5_data_timestamps_array.shape[0] + 1,
                    *h5_data_timestamps_array.shape[1:],
                )
            )
            h5_data_timestamps_array[-1] = even_page_doc["timestamps"][ep_data_key][0]

    def stop(self, doc):
        super().stop(doc)

        h5_bluesky_stop_group = self._h5_output_file[
            self.bluesky_h5_group_name
        ].create_group("stop")
        _copy_metadata_to_h5_datasets(a_mapping=doc, h5_group=h5_bluesky_stop_group)

        # all bluesky documents have been serialized
        # now is the time to create the NeXuS structure
        # parse the "techniques" section of the start document
        start_doc = self.get_start()
        techniques_md = copy.deepcopy(start_doc["md"]["techniques"])
        for technique_info in techniques_md:
            technique = technique_info["technique"]
            # "version" is mandatory
            technique_schema_version = technique_info["version"]
            self.log.info("technique: %s", technique)
            self.log.info("technique version: %s", technique_schema_version)

            _copy_nexus_md_to_nexus_h5(
                nexus_md=technique_info["nxsas"], h5_group=self._h5_output_file
            )

        self.log.info("finished writing file %s", self.filename)

        self._h5_output_file.close()
