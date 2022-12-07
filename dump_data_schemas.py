import os


from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import freeze_support
from platform import system
from tarfile import TarFile, TarInfo
from typing import Dict, List, Optional, TypeVar
from zipfile import ZipFile


from rarfile import RarFile
from tqdm.contrib.concurrent import process_map


import magic


from .exceptions import FileTypeException


Compressed = TypeVar('Compressed', RarFile, ZipFile, TarFile)


MIME_MAP: Dict[str, Compressed] = {
    'application/zip': ZipFile,
    'application/vnd.rar': RarFile,
    'application/x-tar': 'tar',
    'application/gzip': 'gz',
    'application/x-bzip': 'bz',
    'application/x-xz': 'xz'
}


DATA_PATH = None
SCHEMA_PATH = None
PROC_COUNT = None
THREAD_POOL_COUNT = None
SCHEMA_LINES = None


def dump_schemas(data_path: str, schema_path: str, proc_count: int, tp_count: int, lines: int) -> None:
    """
    Dump schemas from each compressed file
    """
    set_globes(data_path, schema_path, proc_count, tp_count, lines)
    if system() == 'Windows':
        freeze_support()
    if not os.path.exists(SCHEMA_PATH):
        os.mkdir(SCHEMA_PATH)
    data_files = os.listdir(DATA_PATH)
    #  Files are independent of each other, done concurrently to improve runtime
    process_map(get_schemas, data_files, max_workers=PROC_COUNT, chunksize=1)
    print('\nDumped schemas')


def get_schemas(file: str) -> None:
    """
    Get the first few lines from each file to determine schema
    (Assumes file content is consistent on each line)
    """
    file_path = os.path.join(DATA_PATH, file)
    #  TODO: add magic header handling
    mime = magic.from_file(file_path, mime=True)
    file_obj = MIME_MAP.get(mime, None)
    if file_obj:
        if isinstance(file_obj, str):
            #  Assumes tar file based on mapping
            if file_obj == 'tar':
                ext = ''
            else:
                ext = file_obj
            file_obj = lambda x, m: TarFile(x, f'{m}:{ext}')

        with file_obj(DATA_PATH+file, 'r') as f:
            f_names = get_filenames(f)
            thread_count = min(len(f_names), THREAD_POOL_COUNT)

            with ThreadPoolExecutor(max_workers=thread_count) as proc_pool:
                for f_name in f_names:
                    proc_pool.submit(get_schema_from_file, f_name, f)

    elif mime == 'text/plain':
        get_schema_from_file(file)
    else:
        raise NotImplementedError(f'no support for {mime}')


def get_schema_from_file(file_name: str, fp: Optional[Compressed]=None) -> None:
    """
    Get lines of file content in a ZIP file
    """
    try:
        if fp:
            if isinstance(fp, TarFile):
                handle = fp.extractfile(file_name)
                if handle is None:
                    print(f'Handle for {file_name} is None, skipping')
                    return
            else:
                handle = fp.open(file_name, 'r')
            if is_directory(fp):
                f_names = get_filenames(fp)
                thread_count = min(len(f_names), THREAD_POOL_COUNT)
                with ThreadPoolExecutor(max_workers=thread_count) as inner_proc_pool:
                    for f_name in f_names:
                        inner_proc_pool.submit(get_schema_from_file, f_name, fp)
        else:
            handle = open(file_name, 'rb')
        eof = get_eof(handle)
        lines = b''.join(handle.readline() for _ in range(SCHEMA_LINES) if handle.tell() != eof)
    except (FileNotFoundError, IOError) as file_error:
        if isinstance(file_error, FileNotFoundError):
            print(f'{file_name} does not exist in {fp.filename}')
        else:
            print('IOError')
        print(f'Exception info: {file_error}')
    else:
        write_schema(file_name, lines)
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
        print(f'IOError occured when writing {file_dest}: {io_e}')
        print('Not writing file')


def get_filenames(f: Compressed) -> List[str]:
    """
    Get files within an archive
    """
    if isinstance(f, (RarFile, ZipFile)):
        return f.namelist()

    if isinstance(f, TarFile):
        return f.getnames()

    raise FileTypeException(f'Unknown filetype: {type(f).__name__}')


def is_directory(file_handle: Compressed) -> bool:
    """
    See if the file is a directory
    """
    if isinstance(file_handle, TarFile):
        info = TarInfo.fromtarfile(file_handle)
        return info.isdir()
    if isinstance(file_handle, ZipFile):
        info = file_handle.getinfo()
        info_bits = info.external_attr >> 16

        #  Check the bits for directory attributes
        return (info_bits & 0x4000) > 0
    if isinstance(file_handle, RarFile):
        info = file_handle.getinfo()
        return info.is_dir()

    raise FileTypeException(f'Unknown filetype: {type(file_handle).__name__}')


def get_eof(fp: Compressed) -> int:
    """
    Get end of file
    """
    fp.seek(-1,2)
    eof = fp.tell()
    fp.seek(0,0)

    return eof


def set_globes(data_path: str, schema_path: str, proc_count: int, tp_count: int, lines: int):
    global DATA_PATH, SCHEMA_PATH, PROC_COUNT, THREAD_POOL_COUNT, SCHEMA_LINES
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
    args = parser.parse_args()

    DATA_PATH = args.dp
    SCHEMA_PATH = args.sp
    PROC_COUNT = args.proc_count
    THREAD_POOL_COUNT = args.thread_count
    SCHEMA_LINES = args.lines

    dump_schemas(args.dp, args.sp, args.proc_count, args.thread_count, args.lines)
    