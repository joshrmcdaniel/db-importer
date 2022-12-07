"""
Script for concurrent zip extraction of a few lines/file to ensure schema consistency
"""

import os


from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from io import BufferedReader, StringIO
from multiprocessing import freeze_support
from platform import system
from tarfile import TarFile, TarInfo
from traceback import format_exc
from typing import Callable, Dict, List, Optional, TypeVar, Union
from zipfile import ZipFile


from rarfile import RarFile
from tqdm.contrib.concurrent import process_map


import magic


from .exceptions import FileTypeException


Compressed = TypeVar('Compressed', RarFile, ZipFile, TarFile)
ExtractedFile = TypeVar('ExtractedFile', RarFile, StringIO, TarFile, ZipFile)

ON_ERROR = Enum('ON_ERROR', ['SKIP', 'WARN', 'RAISE'])


MIME_MAP: Dict[str, Compressed] = {
    'application/zip': ZipFile,
    'application/vnd.rar': RarFile,
    'application/x-tar': '',
    'application/gzip': 'gz',
    'application/x-bzip': 'bz',
    'application/x-xz': 'xz',
    'text/plain': StringIO
}


DATA_PATH = None
ERROR_BEHAVIOR = None
PROC_COUNT = None
SCHEMA_LINES = None
SCHEMA_PATH = None
THREAD_POOL_COUNT = None


def dump_schemas(data_path: str, schema_path: str, proc_count: int, tp_count: int, lines: int) -> None:
    """
    Dump schemas from each compressed file
    """
    global DATA_PATH, SCHEMA_PATH, PROC_COUNT, THREAD_POOL_COUNT, SCHEMA_LINES, ON_ERROR, ERROR_BEHAVIOR
    set_globes(data_path, schema_path, proc_count, tp_count, lines)
    if system() == 'Windows':
        freeze_support()
    if not os.path.exists(SCHEMA_PATH):
        os.mkdir(SCHEMA_PATH)
    try:
        data_files = os.listdir(DATA_PATH)
    except FileNotFoundError as fnfe:
        raise FileNotFoundError(f'Data path not found: {DATA_PATH}') from fnfe

    #  Files are independent of each other, done concurrently to improve runtime
    process_map(get_schemas, data_files, max_workers=PROC_COUNT, chunksize=1)
    print('\nDumped schemas')


def get_schemas(file: str) -> None:
    """
    Get the first few lines from each file to determine schema
    (Assumes file content is consistent on each line)
    """
    file_path = os.path.join(DATA_PATH, file)
    try:
        file_handler = open_file_path(file_path)
    except FileTypeException as fte:
        handle_error(fte)
    else:
        run_thread_pool(get_schema_from_file, get_filenames(file_handler), file_handler)
        file_handler.close()

#  TODO: write func (if even needed)
def process_nested_io_dirs(file_io: Union[Compressed, BufferedReader]):
    """
    Process pool for io dir schema dumping
    """
    if not isinstance(file_io, (RarFile, TarFile, ZipFile)):
        handle_error(NotImplementedError(f'{type(file_io).__name__} not currently supported'))

    run_thread_pool(get_schema_from_file, get_filenames(file_io), file_io)


def get_schema_from_file(file_name: str, fp: Optional[Compressed]=None) -> None:
    """
    Get lines of file content in a ZIP file
    """
    try:
        handle = extract_file(file_name, fp)

        if is_directory(handle) or is_compressed(handle):
            process_nested_io_dirs(handle)
            handle.close()
            return
        if isinstance(handle, TarInfo):
            handle_error(FileTypeException(f'{file_name} returned None on extraction and is not a dir or compressed file'))

        eof = get_eof(handle)
        lines = b''.join(handle.readline() for _ in range(SCHEMA_LINES) if handle.tell() != eof)
    except (FileNotFoundError, IOError, FileTypeException) as file_error:
        handle_error(file_error)

    else:
        try:
            write_schema(file_name, lines)
        except IOError as io_e:
            handle.close()
            handle_error(io_e)
    finally:
        handle.close()


def write_schema(file_path: str, data: bytes):
    """
    Write schema to file
    """
    file_dest = f'{SCHEMA_PATH}{file_path.replace("/", "-sep-")}.schema'
    try:
        with open(file_dest, 'wb') as out_file:
            out_file.write(data)
    except IOError as io_e:
        handle_error(io_e)


def get_filenames(f: Compressed) -> List[str]:
    """
    Get files within an archive
    """
    if isinstance(f, (RarFile, ZipFile)):
        return f.namelist()

    if isinstance(f, TarFile):
        return f.getnames()

    raise FileTypeException(f'Unknown filetype: {type(f).__name__}')


def is_directory(file_handle: Union[Compressed, TarInfo]) -> bool:
    """
    See if the file is a directory
    """
    if isinstance(file_handle, TarFile):
        info = TarInfo.fromtarfile(file_handle)
        return info.isdir()

    if isinstance(file_handle, TarInfo):
        return file_handle.isdir()

    if isinstance(file_handle, ZipFile):
        info = file_handle.getinfo()
        info_bits = info.external_attr >> 16
        #  Check the bits for directory attributes
        return (info_bits & 0x4000) > 0

    if isinstance(file_handle, RarFile):
        info = file_handle.getinfo()
        return info.is_dir()

    if isinstance(file_handle, StringIO):
        return False

    raise FileTypeException(f'Unknown filetype: {type(file_handle).__name__}')


def is_compressed(file_handle: Union[Compressed, StringIO]) -> bool:
    """
    Check if file in handler is compressed
    """
    header = file_handle.read(512)
    file_handle.seek(0, 0)
    mime = magic.from_file(header, mime=True)
    res = MIME_MAP.get(mime, None)
    return res is not None and not isinstance(res, StringIO)


def get_eof(fp: Compressed) -> int:
    """
    Get end of file
    """
    fp.seek(-1,2)
    eof = fp.tell()
    fp.seek(0,0)

    return eof


def open_file_path(file_path: str) -> ExtractedFile:
    """
    Handler for opening supported filetype paths
    """

    mime = magic.from_file(file_path, mime=True)
    file_type = MIME_MAP.get(mime, None)

    if file_type is None:
        raise FileTypeException(f'Unsupported file type: {mime}')

    if isinstance(file_type, str):
        return TarFile(file_path, f'r:{file_type}')

    if isinstance(file_type, RarFile):
        return RarFile(file_path, 'r')

    if isinstance(file_type, ZipFile):
        return ZipFile(file_path, 'r')

    if file_type == StringIO:
        return open(file_path, 'r', encoding='utf-8')


def extract_file(file_name: str, fp: Optional[Compressed]=None) -> Union[ExtractedFile, TarInfo]:
    """
    Handle extraction of multiple file types
    """
    if fp is not None:
        if isinstance(fp, TarFile):
            extracted = fp.extractfile(file_name)
            if extracted is None:
                extracted = fp.getmember(file_name)
            return extracted

        if isinstance(fp, (RarFile, ZipFile)):
            return fp.open(file_name, 'r')

        raise FileTypeException(f'Unknown filetype for {file_name} (in handler {type(fp).__name__})')

    return open(file_name, 'r', encoding='utf-8')


def run_thread_pool(func: Callable[[List[str], Compressed], None], *args: List[Union[List[str], Compressed]]):
    """
    Run extraction method
    """
    thread_count = min(len(args), THREAD_POOL_COUNT)
    with ThreadPoolExecutor(max_workers=thread_count) as proc_pool:
        for filename in args[0]:
            proc_pool.submit(func, filename, args[1])


def handle_error(exc: Exception) -> None:
    """
    Error handling to reduce code bloat
    TODO: find a better way
    """
    if ERROR_BEHAVIOR is ON_ERROR.SKIP:
        return
    if ERROR_BEHAVIOR is ON_ERROR.WARN:
        print(str(exc))
    if ERROR_BEHAVIOR is ON_ERROR.RAISE:
        print(format_exc(exc))
        raise exc


def set_globes(data_path: str, schema_path: str, proc_count: int, tp_count: int, lines: int):
    global DATA_PATH, SCHEMA_PATH, PROC_COUNT, THREAD_POOL_COUNT, SCHEMA_LINES, ON_ERROR, ERROR_BEHAVIOR
    DATA_PATH = data_path
    SCHEMA_PATH = schema_path
    PROC_COUNT = proc_count
    THREAD_POOL_COUNT = tp_count
    SCHEMA_LINES = lines


if __name__ == '__main__':
    parser = ArgumentParser(usage='Dump file schema from zipped files')
    parser.add_argument('data_path', help='Path to data directory', default='./data/', required=False, dest='dp')
    parser.add_argument('-s', '--schema-path', help='Path to output schema', default='./schema/', dest='sp')
    parser.add_argument('-p', '--process-count', help='Concurrent processes to run', type=int, default=10, dest='proc_count')
    parser.add_argument('-t', '--thread-pool-count', help='Threads to use for zip file reading', type=int, default=5, dest='thread_count')
    parser.add_argument('-l', '--lines', help='Lines to read from each file', type=int, default=2, dest='lines')
    parser.add_argument('-e', '--on-error', help='Behavior when an error occures', type=str, default='warn', choices=['skip', 'warn', 'raise'],dest='on_error')
    runtime_args = parser.parse_args()

    DATA_PATH = runtime_args.dp
    SCHEMA_PATH = runtime_args.sp
    PROC_COUNT = runtime_args.proc_count
    THREAD_POOL_COUNT = runtime_args.thread_count
    SCHEMA_LINES = runtime_args.lines
    if runtime_args.on_error == 'skip':
        ERROR_BEHAVIOR = ON_ERROR.SKIP
    elif runtime_args.on_error == 'warn':
        ERROR_BEHAVIOR = ON_ERROR.WARN
    elif runtime_args.on_error == 'raise':
        ERROR_BEHAVIOR = ON_ERROR.RAISE

    dump_schemas(runtime_args.dp, runtime_args.sp, runtime_args.proc_count, runtime_args.thread_count, runtime_args.lines)
