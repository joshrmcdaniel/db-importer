import pytest
import os


from distutils.spawn import find_executable


from .. import dump_data_schemas
from . import test_utils
from .test_data import TestData

TEST_DATA = TestData()

class TestDumpDataSchemas:
    """
    Test cases for schema dumping
    """
    def test_get_filenames_zip(self):
        """
        File name test for zips
        """
        TEST_DATA.zips = test_utils.gen_zips()
        for (zip_f, zip_names) in TEST_DATA.zips:
            filenames_got = dump_data_schemas.get_filenames(zip_f)
            assert filenames_got == zip_names, pytest.fail(reason='Files in ZIP do not match file names expected')

    def test_get_filenames_rar(self):
        """
        File name test for rars
        """
        assert find_executable('rar') is not None, pytest.skip(reason='rar not found on system, cannot test')
        TEST_DATA.rars = test_utils.gen_rars()
        for (rar_f, rar_names) in TEST_DATA.rars:
            filenames_got = dump_data_schemas.get_filenames(rar_f)
            assert filenames_got == rar_names, pytest.fail(reason=f'Files in RAR do not match file names expected\nExpected: {rar_names}\nGot: {filenames_got}')

    def test_get_filenames_tar(self):
        """
        File name test for .tar
        """
        TEST_DATA.tars = test_utils.gen_tars(fmt='')
        for (tar_f, tar_names) in TEST_DATA.tars:
            filenames_got = dump_data_schemas.get_filenames(tar_f)
            assert filenames_got == tar_names, pytest.fail(reason=f'Files in tar do not match file names expected\nExpected: {tar_names}\nGot: {filenames_got}')

    def test_get_filenames_tar_gz(self):
        """
        File name test for .tar.gz
        """
        TEST_DATA.tar_bz2s = test_utils.gen_tars(fmt='bz2')
        for (tar_f, tar_names) in TEST_DATA.tar_bz2s:
            filenames_got = dump_data_schemas.get_filenames(tar_f)
            assert filenames_got == tar_names, pytest.fail(reason=f'Files in bz2 do not match file names expected\nExpected: {tar_names}\nGot: {filenames_got}')

    def test_get_filenames_tar_xz(self):
        """
        File name test for .tar.xz
        """
        pass

    def test_get_filenames_tar_bz(self):
        """
        File name test for .tar.bz
        """
        pass

    def test_get_filenames_tar_bz2(self):
        """
        File name test for .tar.bz2
        """
        pass

    def test_get_filenames_mixed(self):
        """
        File name test for all files
        """
        pass

    def test_get_schema_zip(self):
        pass
        