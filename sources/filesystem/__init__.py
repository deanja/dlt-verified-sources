"""Reads files in s3, gs or azure buckets using fsspec and provides convenience resources for chunked reading of various file formats"""
from typing import Iterator, List, Optional, Tuple, Union

import dlt
from dlt.common.typing import copy_sig
from dlt.sources import DltResource
from dlt.sources.filesystem import FileItem, FileItemDict, fsspec_filesystem, glob_files
from dlt.sources.credentials import FileSystemCredentials

from .helpers import (
    AbstractFileSystem,
    FilesystemConfigurationResource,
    fsspec_from_resource,
)
from .readers import ReadersSource, _read_csv, _read_jsonl, _read_parquet
from .settings import DEFAULT_CHUNK_SIZE


@dlt.source(_impl_cls=ReadersSource, spec=FilesystemConfigurationResource)
def readers(
    bucket_url: str = dlt.secrets.value,
    credentials: Union[FileSystemCredentials, AbstractFileSystem] = dlt.secrets.value,
    file_glob: Optional[str] = "*",
) -> Tuple[DltResource, ...]:
    """This source provides a few resources that are chunked file readers. Readers can be further parametrized before use
       read_csv(chunksize, **pandas_kwargs)
       read_jsonl(chunksize)
       read_parquet(chunksize)

    Args:
        bucket_url (str): The url to the bucket.
        credentials (FileSystemCredentials | AbstractFilesystem): The credentials to the filesystem of fsspec `AbstractFilesystem` instance.
        file_glob (str, optional): The filter to apply to the files in glob format. by default lists all files in bucket_url non-recursively
    """
    return (
        filesystem(bucket_url, credentials, file_glob=file_glob)
        | dlt.transformer(name="read_csv")(_read_csv),
        filesystem(bucket_url, credentials, file_glob=file_glob)
        | dlt.transformer(name="read_jsonl")(_read_jsonl),
        filesystem(bucket_url, credentials, file_glob=file_glob)
        | dlt.transformer(name="read_parquet")(_read_parquet),
    )


@dlt.resource(
    primary_key="file_url", spec=FilesystemConfigurationResource, standalone=True
)
def filesystem(
    bucket_url: str = dlt.secrets.value,
    credentials: Union[FileSystemCredentials, AbstractFileSystem] = dlt.secrets.value,
    file_glob: Optional[str] = "*",
    files_per_page: int = DEFAULT_CHUNK_SIZE,
    extract_content: bool = False,
) -> Iterator[List[FileItem]]:
    """This resource lists files in `bucket_url` using `file_glob` pattern. The files are yielded as FileItem which also
    provide methods to open and read file data. It should be combined with transformers that further process (ie. load files)

    Args:
        bucket_url (str): The url to the bucket.
        credentials (FileSystemCredentials | AbstractFilesystem): The credentials to the filesystem of fsspec `AbstractFilesystem` instance.
        file_glob (str, optional): The filter to apply to the files in glob format. by default lists all files in bucket_url non-recursively
        files_per_page (int, optional): The number of files to process at once, defaults to 100.
        extract_content (bool, optional): If true, the content of the file will be extracted if
            false it will return a fsspec file, defaults to False.

    Returns:
        Iterator[List[FileItem]]: The list of files.
    """
    if isinstance(credentials, AbstractFileSystem):
        fs_client = credentials

        # #301 this too would need the fs_urlpath.
        #   Require it as a parameter if fs is passed in?
        #   Or can we just assume any glob is from fs root?
        #   Otherwise could be messy - need to separate credentials
        #   from the filesystem.
        #   Or run fssspec.url_to_fs() again? - should be 
        #   cheap as fsspec caches filesystem instances.
        #   Initial attempts below

        # #301 hack to get the clean path again. fsspec provides no other public
        # method to get this.
        #_, fs_urlpath = fs_client.url_to_fs(bucket_url, hmmm_how_to_get_credentials)

        # #301 so for now:
        # AbstractFileSystem instances have so state storing their path
        # so we can assume any glob is relative to the filesystem's root.
        # It returns the expected files but:
        # NOTE: bucket_url path is now ignored in dlt.common.storages so user may get
        # longer paths than they want, in length inconsistent for differe calls to this Resource.
        # Need to discuss having bucket url include the glob, and optional explicit path_mask
        # to customise the paths returned.
        fs_urlpath = ""
    else:
        fs_client, fs_urlpath = fsspec_filesystem(bucket_url, credentials)

    files_chunk: List[FileItem] = []
    for file_model in glob_files(fs_client, bucket_url, fs_urlpath, file_glob):
        file_dict = FileItemDict(file_model, credentials)
        if extract_content:
            file_dict["file_content"] = file_dict.read_bytes()
        files_chunk.append(file_dict)  # type: ignore

        # wait for the chunk to be full
        if len(file_dict) >= files_per_page:
            yield files_chunk
            files_chunk = []
    if files_chunk:
        yield files_chunk


read_csv = dlt.transformer(standalone=True)(_read_csv)
read_jsonl = dlt.transformer(standalone=True)(_read_jsonl)
read_parquet = dlt.transformer(standalone=True)(_read_parquet)
