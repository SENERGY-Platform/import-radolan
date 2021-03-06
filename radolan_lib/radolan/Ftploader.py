#  Copyright 2020 InfAI (CC SES)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
import tarfile
from datetime import datetime
from ftplib import FTP
from typing import List, Union, Callable, Type
from import_lib.import_lib import get_logger

import requests

from radolan_lib.radolan.Products import Product, RW, SF, is_known_product
from radolan_lib.util.strings import remove_prefix, remove_suffix

logger = get_logger(__name__)

DWD_HOST = "opendata.dwd.de"


class FtpLoader:
    def __init__(self, product: Type[Product], datadir: str = os.sep + 'tmp' + os.sep + 'radolan'):
        if not os.path.exists(datadir):
            os.mkdir(datadir)
        self.__datadir = datadir
        if not is_known_product(product):
            raise ValueError("Unknown product")
        if product == SF:
            self.__DWD_RECENT_PATH = "climate_environment/CDC/grids_germany/daily/radolan/recent/bin/"
            self.__DWD_HISTORICAL_PATH = "climate_environment/CDC/grids_germany/daily/radolan/historical/bin/"
        if product == RW:
            self.__DWD_RECENT_PATH = "climate_environment/CDC/grids_germany/hourly/radolan/recent/bin/"
            self.__DWD_HISTORICAL_PATH = "climate_environment/CDC/grids_germany/hourly/radolan/historical/bin/"

        self.__DWD_RECENT_URL = "https://" + DWD_HOST + "/" + self.__DWD_RECENT_PATH
        self.__DWD_HISTORICAL_URL = "https://" + DWD_HOST + "/" + self.__DWD_HISTORICAL_PATH

    def download_latest(self) -> str:
        '''
        Downloads the latest radolan file
        :return: Name of the file
        '''
        files = self.__get_recent_list()
        file = files[len(files) - 1]
        return self.__download_recent(self.__datadir, file, self.__DWD_RECENT_URL)

    def download_from_year(self, year: int, max_files: int = None, callback: Callable[[List[str]], any] = None,
                           start: datetime = None) -> Union[List[str], None]:
        '''
        Downloads all files from a given year and return a list of those file.
        You may optionally provide a callback function to be called with a subset of these files.
        If you do so, you will receive smaller subsets of files and don't have to wait for all downloads to be finished
        before further processing

        :param year: Year to download data from
        :param max_files: Upper limit to number of files downloaded. This is a debugging feature
        :param callback: callback function to receive subsets of all files. Any return values will be ignored
        :param start: Optional date restriction. Will not download files with data before this datetime
        :return: List of all files downloaded, if no callback function provided. None, if the callback function received
         the files.
        '''
        if year == datetime.now().year:
            return self.__download_recents(callback, start)
        tarnames = self.__get_files_of_dir(self.__DWD_HISTORICAL_PATH + str(year), "tar.gz")
        if max_files is not None:
            tarnames = tarnames[0:max_files]
        tarnames_filtered = []
        for tarname in tarnames:
            stripped = remove_suffix(tarname, ".tar.gz")
            stripped = remove_prefix(stripped, "SF")
            stripped = remove_prefix(stripped, "RW")
            stripped = remove_prefix(stripped, "-")
            stripped = remove_prefix(stripped, str(year))
            month = int(stripped)
            if start is not None and month < start.month:
                logger.debug("Skipping download for month " + str(month) + " (already imported)")
                continue
            tarnames_filtered.append(tarname)
        files = []
        for tarname in tarnames_filtered:
            targz = self.__download_file(self.__datadir + os.sep + tarname,
                                  self.__DWD_HISTORICAL_URL + str(year) + "/" + tarname)
            tar = tarfile.open(targz, "r:gz")

            logger.info("Extracting local file " + targz)
            tar.extractall(path=self.__datadir)
            names = tar.getnames()
            if len(names) == 1:  # Packed tar in tar.gz, this exists (e.g. first file of 2007)
                tarx = tarfile.open(self.__datadir + os.sep + tar.getnames()[0])
                tarx.extractall(path=self.__datadir)
                names = tarx.getnames()
                os.remove(self.__datadir + os.sep + tar.getnames()[0])

            for name in names:
                if start is not None:
                    if not self.__file_needs_import(start, name):
                        logger.debug("Skipping file (already imported): " + name)
                        continue
                files.append(self.__datadir + os.sep + name)
            os.remove(targz)
            if callback is not None:
                files.sort()
                callback(files)
                files = []

        if callback is not None:
            return None
        files.sort()
        return files

    def __download_recents(self, callback: Callable[[List[str]], any] = None, start: datetime = None) -> Union[
        List[str], None]:
        files = self.__get_recent_list()
        filenames = []
        for f in files:
            if start is not None:
                if not self.__file_needs_import(start, f):
                    logger.debug("Skipping file (already imported): " + f)
                    continue

            filename = self.__download_recent(self.__datadir, f, self.__DWD_RECENT_URL)
            if callback is not None:
                callback([filename])
            filenames.append(filename)
        if callback is not None:
            return None
        return files

    def __get_recent_list(self) -> List[str]:
        return self.__get_files_of_dir(self.__DWD_RECENT_PATH, "bin.gz")

    def __get_files_of_dir(self, dir: str, suffix: str = None) -> List[str]:
        client = FTP(DWD_HOST)
        client.login()
        try:
            client.cwd(dir)
            files = client.nlst()
        except Exception:
            logger.warning("Cloud not fetch files from dir "  + dir)
            return []
        finally:
            client.close()
        if suffix is None:
            return files
        filteredFiles = []
        for f in files:
            if f.endswith(suffix):
                filteredFiles.append(f)
        client.close()
        filteredFiles.sort()
        return filteredFiles

    def __download_recent(self, localdir: str, file: str, remote_path: str) -> str:
        '''
        Downloads the recent radolan file into localdir if it doesn't already exist
        :param localdir: Folder to download to
        :param file: File to download
        :return: Local filename
        '''
        remote_file = remote_path + file
        logger.debug("Downloading " + remote_file)
        local_file = localdir + os.sep + file
        return self.__download_file(local_file=local_file, remote_file=remote_file)

    @staticmethod
    def __download_file(local_file: str, remote_file: str) -> str:
        '''
        Downloads file to dir, if file doesn't already exist
        :param local_file: File to save to
        :param remote_file: Remote file URL
        :return: Local filename
        '''
        if os.path.exists(local_file):
            logger.info("File exists, skipping download: " + local_file)
            return local_file

        logger.info("Downloading remote file " + remote_file)
        with open(local_file, 'wb') as f:
            with requests.get(remote_file, stream=True) as r:
                for chunk in r.iter_content(chunk_size=16 * 1024):
                    f.write(chunk)
        return local_file

    @staticmethod
    def __file_needs_import(start: datetime, f: str) -> bool:
        if start is None:
            return True
        f_trimmed = remove_prefix(f, "raa01-")
        f_trimmed = remove_prefix(f_trimmed, "sf")
        f_trimmed = remove_prefix(f_trimmed, "rw")
        f_trimmed = remove_prefix(f_trimmed, "_10000-")
        f_trimmed = remove_suffix(f_trimmed, ".gz")
        f_trimmed = remove_suffix(f_trimmed, "-dwd---bin")
        try:
            dt_f = datetime.strptime(f_trimmed, "%y%m%d%H%M")
            if dt_f < start:
                return False
        except ValueError:
            logger.error("Datetime of DWD filename could not be parsed. Format changed?"
                         " Filename: " + f + ", Trim attempt: " + f_trimmed)
        return True


if __name__ == "__main__":
    '''
    Simple way of testing the FtpLoader
    '''
    products = [SF, RW]
    for product in products:
        ftp_loader = FtpLoader(product=product)
        if len(ftp_loader.download_latest()) == 0:
            raise Exception
        if len(ftp_loader.download_from_year(2006, 1)) == 0:
            raise Exception
