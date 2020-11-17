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
from typing import List, Tuple, Union

import wradlib
from import_lib.import_lib import ImportLib
from osgeo import osr

from lib.radolan.sf import SFPoint
from lib.radolan.sf.ftploader import FtpLoader


def point_in_bbox(lat: float, long: float, bbox: List[float]) -> bool:
    '''
    Checks if the point is in the bbox
    :param lat: Point lat
    :param long: Point long
    :param bbox: The bounding box as [min Longitude, min Latitude, max Longitude, max Latitude]
    :return: True, if point is the bbox, False otherwise
    '''
    return bbox[0] <= long <= bbox[2] and bbox[1] <= lat <= bbox[3]


def point_in_bboxes(lat: float, long: float, bboxes: List[List[float]]) -> bool:
    '''
    Checks if the point is in at least one bbox
    :param lat: Point lat
    :param long: Point long
    :param bboxes: List of bboxes
    :return: True, if point is in at least one bbox, False otherwise
    '''
    for bbox in bboxes:
        if point_in_bbox(lat, long, bbox):
            return True
    return False


def create_mask(grid: List[List[List[float]]], bboxes: Union[List[List[float]], None]) -> List[Tuple[int, int]]:
    '''
    Creates a list of tuples of grid indices, which fit in the bboxes. If bboxes are none, a list of all tuple indices
    will be returned.
    :param grid: A 3D List of floats representing a indexed coordinate grid. Expected to be rectangular!
    :param bboxes: A list of bounding boxes
    :return: A list of index tuples that represent grid indexes of points that fit into the bounding boxes
    '''
    mask = []
    if bboxes is None:
        for i in range(0, len(grid)):
            for j in range(0, len(grid[0])):
                mask.append((i, j))
        return mask
    for i, val in enumerate(grid):
        for j, xy in enumerate(val):
            if point_in_bboxes(lat=xy[1], long=xy[0], bboxes=bboxes):
                mask.append((i, j))
    return mask


class SFImport:

    def __init__(self, lib: ImportLib):
        '''
        :param lib: Instance of the import-lib
        '''
        self.__lib = lib
        self.__logger = self.__lib.get_logger(__name__)

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

    def import_most_recent(self):
        file = self.__ftp_loader.download_latest()
        if file is None:
            self.__logger.warning("Not importing most recent file: Already exists")
        else:
            try:
                self.__logger.info('Imported ' + str(self.importFile(file)) + ' points from most recent data')
            except OSError as e:
                self.__logger.error("Could not import file " + file + "due to: " + str(e))

    def import_from_year(self, year: int):
        if year < 2006:
            raise ValueError("Year may not be smaller than 2006")
        self.__ftp_loader.download_from_year(year, callback=self.importFiles)

    def importFile(self, file: str, delete_file: bool = True) -> int:
        data, metadata = wradlib.io.read_radolan_composite(file)

        nodataflag = metadata['nodataflag']
        datetime = metadata['datetime']
        precision = metadata['precision']
        points = 0

        for ij in self.__mask:
            val = data[ij[0]][ij[1]]
            if val != nodataflag:
                position_projected = self.__radolan_grid_ll[ij[0]][ij[1]]

                point = SFPoint.get_message(pos_long=position_projected[0], pos_lat=position_projected[1],
                                            epsg=self.__epsg,
                                            val_tenth_mm_d=val,
                                            precision=precision)
                self.__lib.put(datetime, point)
                self.__logger.debug(point)
                points += 1

        if delete_file:
            os.remove(file)
        return points

    def importFiles(self, files: List[str], delete_files: bool = True) -> int:
        counter = 0
        for file in files:
            counter += self.importFile(file, delete_files)
        return counter
