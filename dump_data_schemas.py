import magic
import os


from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import freeze_support
from rarfile import RarFile
from tqdm.contrib.concurrent import process_map
from tarfile import TarFile
from typing import Dict, List, TypeVar, Union
from zipfile import ZipFile


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
    set_globes(data_path, schema_path, proc_count, tp_count, lines)
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
            if ext == 'tar':
                ext = ''
            file_obj = lambda x, m: TarFile(x, f'{m}:{ext}')
        with file_obj(DATA_PATH+file, 'r') as f:
            with ThreadPoolExecutor(max_workers=THREAD_POOL_COUNT) as proc_pool:
                f_names = get_filenames(f)
                for f_name in f_names:
                    proc_pool.submit(get_schema_from_file, f_name, f)
    elif mime == 'text/plain':
        get_schema_from_file(file)
    else:
        raise NotImplementedError(f'no support for {mime}')


def get_schema_from_file(file_name: str, fp: Union[Compressed, None]=None) -> None:
    """
    Get lines of file content in a ZIP file
    """
    try:
        if fp:
            if isinstance(fp, TarFile):
                handle = fp.extractfile(file_name)
            else:
                handle = fp.open(file_name, 'r')
        else:
            handle = open(file_name, 'rb')
        handle.seek(-1,2)
        eof = handle.tell()
        handle.seek(0,0)
        lines = b''.join(handle.readline() for _ in range(SCHEMA_LINES) if handle.tell() != eof)
    except (FileNotFoundError, IOError) as fe:
        if isinstance(fe, FileNotFoundError):
            print(f'{file_name} does not exist in {fp.filename}')
        else:
            print('IOError')
        print(f'Exception info: {fe}')
    else:
        with open(f'{SCHEMA_PATH}{file_name}.schema', 'wb') as wf:
            wf.write(lines)
    finally:
        handle.close()


def get_filenames(f: Union[RarFile, TarFile, ZipFile]) -> List[str]:
    if isinstance(f, (RarFile, ZipFile)):
        return f.namelist()
    elif isinstance(f, TarFile):
        return f.getnames()


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
    dump_schemas(args.dp, args.sp, args.proc_count, args.thread_count, args.lines)