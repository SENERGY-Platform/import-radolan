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
from typing import List

import numpy as np
import wradlib
from confluent_kafka import Producer
from osgeo import osr

from lib.radolan.sf import SFPoint
from lib.radolan.sf.ftploader import FtpLoader

import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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


class SFImport:

    def __init__(self, producer: Producer, topic: str, epsg: int, import_id: str, bboxes: List[List[float]] = None):
        '''

        :param producer: Kafka Producer
        :param topic: Output Topic
        :param epsg: ESPG Code to use for projection
        :param import_id: ID annotation for kafka exports. Used as key for messages as well
        :param bboxes: List of bboxes to select specific areas for import. If not set, all areas will be imported
        '''
        self.__proj_radolan = wradlib.georef.create_osr("dwd-radolan")
        self.__proj_ll = osr.SpatialReference()
        self.__proj_ll.ImportFromEPSG(epsg)
        self.__epsg = epsg
        self.__import_id = import_id
        self.__producer = producer
        self.__topic = topic
        self.__bboxes = bboxes
        self.__ftp_loader = FtpLoader()

    def import_most_recent(self):
        file = self.__ftp_loader.download_latest()
        if file is None:
            logger.warning("Not importing most recent file: Already exists")
        else:
            try:
                logger.info('Imported ' + str(self.importFile(file)) + ' points from most recent data')
            except OSError as e:
                logger.error("Could not import file " + file + "due to: " + str(e))

    def import_from_year(self, year: int):
        if year < 2006:
            raise ValueError("Year may not be smaller than 2006")
        self.__ftp_loader.download_from_year(year, callback=self.importFiles)

    def importFile(self, file: str, delete_file: bool = True) -> int:
        data, metadata = wradlib.io.read_radolan_composite(file)

        radolan_grid_xy = wradlib.georef.get_radolan_grid(data.shape[0], data.shape[1])
        radolan_grid_ll = wradlib.georef.reproject(radolan_grid_xy, projection_source=self.__proj_radolan,
                                                   projection_target=self.__proj_ll)


        nodataflag = metadata['nodataflag']
        datetime = metadata['datetime']
        precision = metadata['precision']
        points = 0

        it = np.nditer(data, flags=['multi_index'])
        for val in it:
            if val != nodataflag:
                position_projected = radolan_grid_ll[it.multi_index[0]][it.multi_index[1]]
                if self.__bboxes is None or point_in_bboxes(position_projected[1], position_projected[0],
                                                            self.__bboxes):
                    point = SFPoint.get_message(pos_long=position_projected[0], pos_lat=position_projected[1],
                                                epsg=self.__epsg,
                                                datetime=datetime,
                                                val_tenth_mm_d=data[it.multi_index[0]][it.multi_index[1]],
                                                precision=precision, import_id=self.__import_id)
                    self.__producer.produce(self.__topic, key=self.__import_id, value=point)
                    logger.debug(point)
                    points += 1

        if delete_file:
            os.remove(file)
        self.__producer.flush()
        return points

    def importFiles(self, files: List[str], delete_files: bool = True) -> int:
        counter = 0
        for file in files:
            counter += self.importFile(file, delete_files)
        return counter
