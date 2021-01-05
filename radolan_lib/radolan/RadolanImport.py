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
from typing import List, Type

import wradlib
from import_lib.import_lib import ImportLib, get_logger
from osgeo import osr

from radolan_lib.radolan.Products import Product, RW, SF, is_known_product
from radolan_lib.util.bbox import create_mask
from radolan_lib.radolan import Point
from radolan_lib.radolan.Ftploader import FtpLoader


class RadolanImport:

    def __init__(self, lib: ImportLib, product: Type[Product]):
        '''
        :param lib: Instance of the import-lib
        :param product: A radolan product
        '''

        if not is_known_product(product):
            raise ValueError("Unknown product")
        self.__product = product
        history_length = 0
        dim_x, dim_y = 900, 900
        if self.__product == SF:
            history_length = 72
            dim_x, dim_y = 900, 900
        if self.__product == RW:
            history_length = 12
            dim_x, dim_y = 900, 900

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
        self.__ftp_loader = FtpLoader(product=self.__product)
        radolan_grid_xy = wradlib.georef.get_radolan_grid(dim_x, dim_y)
        self.__radolan_grid_ll = wradlib.georef.reproject(radolan_grid_xy, projection_source=self.__proj_radolan,
                                                          projection_target=self.__proj_ll)
        self.__logger.debug("Preparing mask...")
        self.__mask = create_mask(self.__radolan_grid_ll, self.__bboxes)

        history = self.__lib.get_last_n_messages(history_length * len(self.__mask))  # Last messages for each location

    def import_most_recent(self):
        file = self.__ftp_loader.download_latest()
        try:
            self.__logger.info('Imported ' + str(self.import_file(file)) + ' points from most recent data')
        except OSError as e:
            self.__logger.error("Could not import file " + file + "due to: " + str(e))

    def import_from_year(self, year: int, start: datetime = None):
        if year < 2006 and isinstance(self.__product, SF):
            raise ValueError("Year may not be smaller than 2006")
        if year < 2005 and isinstance(self.__product, RW):
            raise ValueError("Year may not be smaller than 2005")
        self.__ftp_loader.download_from_year(year, callback=self.import_files, start=start)

    def import_file(self, file: str, delete_file: bool = True) -> int:
        try:
            data, metadata = wradlib.io.read_radolan_composite(file)
        except (OSError, ValueError) as e:
            self.__logger.warning(str(e) + " Skipping file! This is most likely caused by invalid DWD data")
            return 0

        nodataflag = metadata['nodataflag']
        datetime = metadata['datetime']
        precision = metadata['precision']
        points = 0

        for i, j in self.__mask:
            val = round(data[i][j], 2)
            if val != nodataflag:
                position_projected = self.__radolan_grid_ll[i][j]
                lat = position_projected[1]
                long = position_projected[0]

                warn_event = -1
                warn_level = ""
                unit = ""

                if self.__product == SF:
                    unit = "mm/d"
                if self.__product == RW:
                    unit = "mm/h"

                point = Point.get_message(pos_long=long, pos_lat=lat,
                                          epsg=self.__epsg,
                                          value=val,
                                          precision=precision,
                                          unit=unit)
                self.__lib.put(datetime, point)
                self.__logger.debug(str(datetime) + ":" + str(point))
                points += 1

        if delete_file:
            os.remove(file)
        return points

    def import_files(self, files: List[str], delete_files: bool = True) -> int:
        counter = 0
        for file in files:
            counter += self.import_file(file, delete_files)
        return counter
