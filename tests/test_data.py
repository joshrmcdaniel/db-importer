from dataclasses import dataclass
from typing import List, Tuple, TypeVar, Union


from tarfile import TarFile
from zipfile import ZipFile


from rarfile import RarFile


__test__ = False


TestArchive = TypeVar('TestArchive', List[Tuple[Union[RarFile, TarFile, ZipFile], List[str]]], None)


@dataclass
class TestData:
    """
    To generate data once per test and store their values
    for easy access
    """
    def __init__(self,
        rars: TestArchive=None,
        tars: TestArchive=None,
        tar_bz2s: TestArchive=None,
        tar_gzs: TestArchive=None,
        tar_xzs: TestArchive=None,
        zips: TestArchive=None
    ) -> None:
        self.rars: TestArchive = rars
        self.tars: TestArchive = tars
        self.tar_bz2s: TestArchive = tar_bz2s
        self.tar_gzs: TestArchive = tar_gzs
        self.tar_xzs: TestArchive = tar_xzs
        self.zips: TestArchive = zips

    @property
    def files(self) -> List[TestArchive]:
        """
        Get all files regardless of filetype
        """
        return [
            *self.rars,
            *self.tars,
            *self.tar_bz2s,
            *self.tar_gzs,
            *self.tar_xzs,
            *self.zips
        ]
