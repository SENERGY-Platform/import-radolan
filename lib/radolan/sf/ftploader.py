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

import logging
import os
import tarfile
from datetime import datetime
from ftplib import FTP
from typing import List, Union, Callable

import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DWD_HOST = "opendata.dwd.de"
DWD_RECENT_PATH = "climate_environment/CDC/grids_germany/daily/radolan/recent/bin/"
DWD_RECENT_URL = "https://" + DWD_HOST + "/" + DWD_RECENT_PATH
DWD_HISTORICAL_PATH = "climate_environment/CDC/grids_germany/daily/radolan/historical/bin/"
DWD_HISTORICAL_URL = "https://" + DWD_HOST + "/" + DWD_HISTORICAL_PATH


def download_recent(localdir: str, file: str) -> Union[str, None]:
    '''
    Downloads the recent radolan sf file into localdir if it doesn't already exist
    :param localdir: Folder to download to
    :param file: File to download
    :return: Local filename or None if file exists already
    '''
    remote_file = DWD_RECENT_URL + file
    logger.debug("Downloading " + remote_file)
    local_file = localdir + os.sep + file
    return download_file(local_file=local_file, remote_file=remote_file)


def download_file(local_file: str, remote_file: str) -> Union[str, None]:
    '''
    Downloads file to dir, if file doesn't already exist
    :param local_file: File to save to
    :param remote_file: Remote file URL
    :return: Local filename or None if file exists already
    '''
    if os.path.exists(local_file):
        logger.warning("File exists, skipping download: " + local_file)
        return None

    logger.info("Downloading remote file " + remote_file)
    with open(local_file, 'wb') as f:
        with requests.get(remote_file, stream=True) as r:
            for chunk in r.iter_content(chunk_size=16 * 1024):
                f.write(chunk)
    return local_file


class FtpLoader:
    def __init__(self, datadir: str = os.sep + 'tmp' + os.sep + 'radolan-sf'):
        if not os.path.exists(datadir):
            os.mkdir(datadir)
        self.__datadir = datadir

    def download_latest(self) -> Union[str, None]:
        '''
        Downloads the latest radolan sf file
        :return: Name of the file or None, if the file already exists
        '''
        files = self.__get_recent_list()
        file = files[len(files) - 1]
        return download_recent(self.__datadir, file)

    def download_from_year(self, year: int, max_files: int = None, callback: Callable[[List[str]], any] = None) -> Union[List[str], None]:
        '''
        Downloads all files from a given year and return a list of those file.
        You may optionally provide a callback function to be called with a subset of these files.
        If you do so, you will receive smaller subsets of files and don't have to wait for all downloads to be finished
        before further processing
        :param year: Year to download data from
        :param max_files: Upper limit to number of files downloaded. This is a debugging feature
        :param callback: callback function to receive subsets of all files. Any return values will be ignored
        :return: List of all files downloaded, if no callback function provided. None, if the callback function received
         the files.
        '''
        if year == datetime.now().year:
            return self.__download_recents(callback)
        tarnames = self.__get_files_of_dir(DWD_HISTORICAL_PATH + str(year), "tar.gz")
        if max_files is not None:
            tarnames = tarnames[0:max_files]
        files = []
        for tarname in tarnames:
            targz = download_file(self.__datadir + os.sep + tarname, DWD_HISTORICAL_URL + str(year) + "/" + tarname)
            if targz is not None:
                tar = tarfile.open(targz, "r:gz")

                logger.debug("Extracting local file " + targz)
                tar.extractall(path=self.__datadir)
                names = tar.getnames()
                if len(names) == 1:  # Packed tar in tar.gz, this exists (e.g. first file of 2007)
                    tarx = tarfile.open(self.__datadir + os.sep + tar.getnames()[0])
                    tarx.extractall(path=self.__datadir)
                    names = tarx.getnames()
                    os.remove(self.__datadir + os.sep + tar.getnames()[0])

                for name in names:
                    files.append(self.__datadir + os.sep + name)
                os.remove(targz)
                if callback is not None:
                    callback(files)
                    files = []

        if callback is not None:
            return None
        return files

    def __download_recents(self, callback: Callable[[List[str]], any] = None) -> Union[List[str], None]:
        files = self.__get_recent_list()
        filenames = []
        for f in files:
            filename = download_recent(self.__datadir, f)
            if callback is not None:
                callback([filename])
            filenames.append(filename)
        if callback is not None:
            return None
        return files

    def __get_recent_list(self) -> List[str]:
        return self.__get_files_of_dir(DWD_RECENT_PATH, "bin.gz")

    def __get_files_of_dir(self, dir: str, suffix: str = None) -> List[str]:
        client = FTP(DWD_HOST)
        client.login()
        client.cwd(dir)
        files = client.nlst()
        client.close()
        if suffix is None:
            return files
        filteredFiles = []
        for f in files:
            if f.endswith(suffix):
                filteredFiles.append(f)
        client.close()
        return filteredFiles    


if __name__ == "__main__":
    ftp_loader = FtpLoader()
    ftp_loader.download_latest()
