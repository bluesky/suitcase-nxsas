# Suitcase subpackages should follow strict naming and interface conventions.
# The public API must include Serializer and should include export if it is
# intended to be user-facing. They should accept the parameters sketched here,
# but may also accpet additional required or optional keyword arguments, as
# needed.
import event_model
from pathlib import Path
import suitcase.utils
from ._version import get_versions

__version__ = get_versions()['version']
del get_versions


def export(gen, directory, file_prefix='{uid}-', **kwargs):
    """
    Export a stream of documents to sas.

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

        In advanced applications, this may direct the serialized output to a
        memory buffer, network socket, or other writable buffer. It should be
        an instance of ``suitcase.utils.MemoryBufferManager`` and
        ``suitcase.utils.MultiFileManager`` or any object implementing that
        interface. See the suitcase documentation at
        https://nsls-ii.github.io/suitcase for details.

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


class Serializer(event_model.DocumentRouter):
    """
    Serialize a stream of documents to sas.

    .. note::

        This can alternatively be used to write data to generic buffers rather
        than creating files on disk. See the documentation for the
        ``directory`` parameter below.

    Parameters
    ----------
    directory : string, Path, or Manager
        For basic uses, this should be the path to the output directory given
        as a string or Path object. Use an empty string ``''`` to place files
        in the current working directory.

        In advanced applications, this may direct the serialized output to a
        memory buffer, network socket, or other writable buffer. It should be
        an instance of ``suitcase.utils.MemoryBufferManager`` and
        ``suitcase.utils.MultiFileManager`` or any object implementing that
        interface. See the suitcase documentation at
        https://nsls-ii.github.io/suitcase for details.

    file_prefix : str, optional
        The first part of the filename of the generated output files. This
        string may include templates as in ``{proposal_id}-{sample_name}-``,
        which are populated from the RunStart document. The default value is
        ``{uid}-`` which is guaranteed to be present and unique. A more
        descriptive value depends on the application and is therefore left to
        the user.

    **kwargs : kwargs
        Keyword arugments to be passed through to the underlying I/O library.

    Attributes
    ----------
    artifacts
        dict mapping the 'labels' to lists of file names (or, in general,
        whatever resources are produced by the Manager)
    """
    def __init__(self, directory, file_prefix='{uid}-', **kwargs):

        self._file_prefix = file_prefix
        self._kwargs = kwargs
        self._templated_file_prefix = ''  # set when we get a 'start' document

        if isinstance(directory, (str, Path)):
            # The user has given us a filepath; they want files.
            # Set up a MultiFileManager for them.
            self._manager = suitcase.utils.MultiFileManager(directory)
        else:
            # The user has given us their own Manager instance. Use that.
            self._manager = directory

        # Finally, we usually need some state related to stashing file
        # handles/buffers. For a Serializer that only needs *one* file
        # this may be:
        #
        # self._output_file = None
        #
        # For a Serializer that writes a separate file per stream:
        #
        # self._files = {}

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
        return self._manager.artifacts

    def close(self):
        """
        Close all of the resources (e.g. files) allocated.
        """
        self._manager.close()

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
    #
    # In each of these methods:
    #
    # - If needed, obtain a new file/buffer from the manager and stash it
    #   on instance state (self._files, etc.) if you will need it again
    #   later. Example:
    #
    #   filename = f'{self._templated_file_prefix}-primary.csv'
    #   file = self._manager.open('stream_data', filename, 'xt')
    #   self._files['primary'] = file
    #
    #   See the manager documentation below for more about the arguments to open().
    #
    # - Write data into the file, usually something like:
    #
    #   content = my_function(doc)
    #   file.write(content)
    #
    #   or
    #
    #   my_function(doc, file)

    def start(self, doc):
        # Fill in the file_prefix with the contents of the RunStart document.
        # As in, '{uid}' -> 'c1790369-e4b2-46c7-a294-7abfa239691a'
        # or 'my-data-from-{plan-name}' -> 'my-data-from-scan'
        self._templated_file_prefix = self._file_prefix.format(**doc)
        ...

    def descriptor(self, doc):
        ...

    def event_page(self, doc):
        # There are other representations of Event data -- 'event' and
        # 'bulk_events' (deprecated). But that does not concern us because
        # DocumentRouter will convert this representations to 'event_page'
        # then route them through here.
        ...

    def stop(self, doc):
        ...
