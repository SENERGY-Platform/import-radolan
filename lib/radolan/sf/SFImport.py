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
from datetime import datetime
from typing import List

import wradlib
from import_lib.import_lib import ImportLib, get_logger
from osgeo import osr

from lib.bbox import create_mask
from lib.radolan.sf import SFPoint
from lib.radolan.sf.Annotator import Annotator
from lib.radolan.sf.ftploader import FtpLoader


class SFImport:

    def __init__(self, lib: ImportLib):
        '''
        :param lib: Instance of the import-lib
        '''
        self.__lib = lib
        self.__logger = get_logger(__name__)

        self.__proj_radolan = wradlib.georef.create_osr("dwd-radolan")
        self.__proj_ll = osr.SpatialReference()
        epsg = self.__lib.get_config("EPSG", 4326)
        self.__proj_ll.ImportFromEPSG(epsg)
        self.__epsg = epsg

        self.__bboxes = self.__lib.get_config("BBOXES", None)
        if not isinstance(self.__bboxes, List):
            self.__logger.error("Invalid config for BBOXES will not be used")
            self.__bboxes = None
        self.__ftp_loader = FtpLoader()
        radolan_grid_xy = wradlib.georef.get_radolan_grid(900, 900)
        self.__radolan_grid_ll = wradlib.georef.reproject(radolan_grid_xy, projection_source=self.__proj_radolan,
                                                          projection_target=self.__proj_ll)
        self.__logger.debug("Preparing mask...")
        self.__mask = create_mask(self.__radolan_grid_ll, self.__bboxes)

        history = self.__lib.get_last_n_messages(72 * len(self.__mask))  # Last 72 messages for each location
        self.__annotator = Annotator(history)

    def import_most_recent(self):
        file = self.__ftp_loader.download_latest()
        if file is None:
            self.__logger.warning("Not importing most recent file: Already exists")
        else:
            try:
                self.__logger.info('Imported ' + str(self.importFile(file)) + ' points from most recent data')
            except OSError as e:
                self.__logger.error("Could not import file " + file + "due to: " + str(e))

    def import_from_year(self, year: int, start: datetime = None):
        if year < 2006:
            raise ValueError("Year may not be smaller than 2006")
        self.__ftp_loader.download_from_year(year, callback=self.importFiles, start=start)

    def importFile(self, file: str, delete_file: bool = True) -> int:
        data, metadata = wradlib.io.read_radolan_composite(file)

        nodataflag = metadata['nodataflag']
        datetime = metadata['datetime']
        precision = metadata['precision']
        points = 0

        for i, j in self.__mask:
            val = round(data[i][j] * 0.1, 2)  # val in 1/10 mm/d needs to be converted in 1 mm7d
            precision = round(precision * 0.1 , 2)  # precision in 1/10 mm/d needs to be converted in 1 mm7d
            if val != nodataflag:
                position_projected = self.__radolan_grid_ll[i][j]
                lat = position_projected[1]
                long = position_projected[0]

                warn_level, warn_event = self.__annotator.get_warn_event_level(datetime, lat, long, val)

                point = SFPoint.get_message(pos_long=long, pos_lat=lat,
                                            epsg=self.__epsg,
                                            value=val,
                                            precision=precision,
                                            warn_level=warn_level,
                                            warn_event=warn_event)
                self.__lib.put(datetime, point)
                self.__logger.debug(str(datetime) + ":" + str(point))
                points += 1

        if delete_file:
            os.remove(file)
        return points

    def importFiles(self, files: List[str], delete_files: bool = True) -> int:
        counter = 0
        for file in files:
            counter += self.importFile(file, delete_files)
        return counter
