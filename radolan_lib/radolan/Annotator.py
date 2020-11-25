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

from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Dict

from import_lib.import_lib import get_logger

from radolan_lib.radolan.HistoryManager import HistoryManager
from radolan_lib.radolan.Point import extract_message

logger = get_logger(__name__)

dauerregen = {
    -1: "Keine Aussage möglich",
    0: "",
    2: "Dauerregen",
    3: "Ergiebiger Dauerregen",
    4: "Extrem ergiebiger Dauerregen"
}

starkregen = {
    -1: "Keine Aussage möglich",
    0: "",
    2: "Starkregen",
    3: "Heftiger Starkregen",
    4: "Extrem heftiger Starkregen"
}


class Annotator:
    def __init__(self, history: Optional[List[Tuple[datetime, Dict]]]):
        '''
        Init the Annotator with a list of historic messages

        :param history: List of tuples with datetime and message
        '''
        self.__history = HistoryManager()
        if history is not None:
            for date_time, msg in history:
                try:
                    lat, long, value, _, _, _, _, _ = extract_message(msg)
                except ValueError:
                    logger.warning("Malformed message ignored")
                    continue
                self.__history.add_point(date_time, lat, long, value)

    def get_dauerregen(self, date_time: datetime, lat: float, long: float, value: float) -> Tuple[int, str]:
        '''
        Register a message and get the highest warn level taking historic values into account

        :param date_time: datetime
        :param lat: latitude
        :param long: longitude
        :param value: value in mm/d
        :return: The highest warning level als int and str representation
        '''

        self.__history.add_point(date_time, lat, long, value)

        level_24, level_48, level_72 = 0, 0, 0

        level_24 = self.__classify_dauerregen(value, 24)

        value_48 = self.__history.get_value(date_time - timedelta(days=1), lat, long)
        if value_48 is not None:
            level_48 = self.__classify_dauerregen(value + value_48, 48)
            value_72 = self.__history.get_value(date_time - timedelta(days=2), lat, long)
            if value_72 is not None:
                level_72 = self.__classify_dauerregen(value + value_48 + value_72, 72)
            else:
                logger.debug("Can't ensure correct warn_level, missing historic data")
                level_72 = -1
        else:
            logger.debug("Can't ensure correct warn_level, missing historic data")
            level_48 = -1

        self.__history.remove_older_than(date_time - timedelta(days=2), lat, long)

        level = -1
        if level_48 != -1 and level_72 != -1:
            level = max(level_24, level_48, level_72)
        return level, dauerregen[level]

    def get_starkregen(self, date_time: datetime, lat: float, long: float, value: float) -> Tuple[int, str]:
        '''
        Register a message and get the highest warn level taking historic values into account

        :param date_time: datetime
        :param lat: latitude
        :param long: longitude
        :param value: value in mm/h
        :return: The highest warning level als int and str representation
        '''

        self.__history.add_point(date_time, lat, long, value)

        level_1 = self.__classify_starkregen(value, 1)

        values_2_5 = (
            self.__history.get_value(date_time - timedelta(hours=1), lat, long),
            self.__history.get_value(date_time - timedelta(hours=2), lat, long),
            self.__history.get_value(date_time - timedelta(hours=3), lat, long),
            self.__history.get_value(date_time - timedelta(hours=4), lat, long),
            self.__history.get_value(date_time - timedelta(hours=5), lat, long),
        )
        if None in values_2_5:
            logger.debug("Can't ensure correct warn_level, missing historic data")
            level_6 = -1
        else:
            value_6 = value + sum(values_2_5)
            level_6 = self.__classify_starkregen(value_6, 1)

        self.__history.remove_older_than(date_time - timedelta(hours=5), lat, long)

        level = -1
        if level_6 != -1:
            level = max(level_1, level_6)
        return level, dauerregen[level]

    @staticmethod
    def __classify_dauerregen(value: float, hours: int) -> int:
        '''
        Classify a value

        :param value: total precipitation within hours
        :param hours: time interval (valid are 12, 24, 48, 72)
        :return: highest warning level
        :except ValueError: if supplied invalid hours
        '''
        if hours == 12:
            if value < 25:
                return 0
            if value < 40:
                return 2
            if value < 70:
                return 3
            return 4
        if hours == 24:
            if value < 30:
                return 0
            if value < 50:
                return 2
            if value < 80:
                return 3
            return 4
        if hours == 48:
            if value < 40:
                return 0
            if value < 60:
                return 2
            if value < 90:
                return 3
            return 4
        if hours == 72:
            if value < 60:
                return 0
            if value < 90:
                return 2
            if value < 120:
                return 3
            return 4
        raise ValueError

    @staticmethod
    def __classify_starkregen(value: float, hours: int) -> int:
        '''
        Classify a value

        :param value: total precipitation within hours
        :param hours: time interval (valid are 1, 6)
        :return: highest warning level
        :except ValueError: if supplied invalid hours
        '''
        if hours == 1:
            if value < 15:
                return 0
            if value < 25:
                return 2
            if value < 40:
                return 3
            return 4
        if hours == 6:
            if value < 20:
                return 0
            if value < 35:
                return 2
            if value < 60:
                return 3
            return 4
        raise ValueError