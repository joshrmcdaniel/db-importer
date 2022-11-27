import pytest
import os


from distutils.spawn import find_executable


from .. import dump_data_schemas
from . import test_utils


class TestDumpDataSchemas:
    def test_get_filenames_zip(self):
        random_zips = test_utils.gen_zips()
        for (zip_f, zip_names) in random_zips:
            assert dump_data_schemas.get_filenames(zip_f) == zip_names, pytest.fail(reason='Files in ZIP do not match file names expected')

    def test_get_filenames_rar(self):
        assert find_executable('rar') is not None, pytest.skip(reason='rar not found on system, cannot test')
        random_rars = test_utils.gen_rars(1)
        for (rar_f, rar_names) in random_rars:
            filenames_got = dump_data_schemas.get_filenames(rar_f)
            assert filenames_got == rar_names, pytest.fail(reason=f'Files in RAR do not match file names expected\nExpected: {rar_names}\nGot: {filenames_got}')
        