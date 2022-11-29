import os
import random
import shutil
import string
import tarfile


from io import BytesIO
from pathlib import PosixPath
from typing import List, Tuple, Union
from zipfile import ZipFile


from rarfile import RarFile


__test__ = False


CHARS = string.ascii_uppercase+string.ascii_lowercase+string.digits


def gen_random_str(str_len: int=10, fmt='bytes') -> Union[bytes, str]:
    """
    Generate random string
    """
    rand_str = "".join(random.choice(CHARS) for _ in range(str_len))
    if fmt == 'bytes':
        return ascii(rand_str)
    elif fmt == 'utf-8':
        return rand_str
    else:
        raise NotImplementedError('unsupported format')


def gen_zips(amt: int=0) -> List[Tuple[ZipFile, List[str]]]:
    """
    Generate random zips with random file names
    """
    zips = []
    if amt == 0:
        amt = random.randint(10, 25)

    for _ in range(amt):
        tmp_zip = ZipFile(BytesIO(), 'w')
        tmp_names = []
        for _ in range(random.randint(5,25)):
            tmp_name = gen_random_str(random.randint(6,15))
            with tmp_zip.open(tmp_name, 'w') as tmp_hdlr:
                tmp_hdlr.write(b'TESTING-'+bytes(tmp_name, 'ascii'))
                tmp_names.append(tmp_name)

        zips.append((tmp_zip, tmp_names))

    return zips


def gen_rars(amt: int=0) -> List[Tuple[RarFile, List[str]]]:
    """
    Generate random rars with random file names
    """
    rars = []
    if amt == 0:
        amt = random.randint(10, 25)
    tmp_path = PosixPath('tests/PYTEST_TMPDIR/rar-test')
    if tmp_path.exists():
        shutil.rmtree(tmp_path)
    tmp_path.mkdir(parents=True)
    for i in range(amt):
        tmp_dir = tmp_path.joinpath(ascii(i))
        if not tmp_dir.exists():
            tmp_dir.mkdir(parents=True)
        tmp_names = []
        for _ in range(random.randint(5,25)):
            tmp_name = gen_random_str(random.randint(6,15), fmt='utf-8')
            tmp_fp = tmp_dir / rf"{tmp_name}"
            tmp_hdlr = tmp_fp.open('w')
            tmp_hdlr.write(rf"TESTING-{tmp_name}")
            tmp_names.append(str(tmp_fp))
            tmp_hdlr.close()
        print(tmp_dir)
        os.system(f'rar a {tmp_dir!s}.rar {tmp_dir}/*')
        rars.append((RarFile(f'{tmp_dir!s}.rar', 'r'), sorted(tmp_names)))

    return rars


def gen_tars(amt: int=0, /, fmt: str='') -> List[Tuple[tarfile.TarFile, List[str]]]:
    """
    Generate random tars with random file names
    """
    tars = []
    tar_fmts = ('bz2', 'gz', 'xz', '', '*')
    assert fmt in tar_fmts, f'Invalid format {fmt}. Supported formats: {", ".join(tar_fmts)}'
    if amt == 0:
        amt = random.randint(10, 25)

    for _ in range(amt):
        tmp_tar = tarfile.open(fileobj=BytesIO(), mode=f'w:{fmt}')
        tmp_names = []
        for _ in range(random.randint(5,25)):
            tmp_name = gen_random_str(random.randint(6,15))
            tmp_data = BytesIO(initial_bytes=bytearray(tmp_name, 'ascii'))
            tmp_tf = tarfile.TarInfo(tmp_data)
            tmp_tar.addfile(tmp_tf, tmp_data)
            tmp_names.append(tmp_name)

        tars.append((tmp_tar, tmp_names))

    return tars
