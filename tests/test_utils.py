import os
import random
import pytest
import shutil
import string

from io import BytesIO
from pathlib import PosixPath
from rarfile import RarFile
from typing import List, Tuple, Union
from zipfile import ZipFile

CHARS = string.ascii_uppercase+string.ascii_lowercase+string.digits


def gen_random_str(str_len: int=10, fmt='bytes') -> Union[bytes, str]:
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
            tmp_hdlr = tmp_zip.open(tmp_name, 'w')
            tmp_hdlr.write(b'TESTING-'+bytes(tmp_name, 'ascii'))
            tmp_names.append(tmp_name)
            tmp_hdlr.close()
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