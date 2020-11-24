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

from datetime import datetime
from typing import List, Tuple, Optional


class HistoryManager:
    def __init__(self, history: List[Tuple[datetime, float, float, float]] = None):
        '''
        :param history: list of historic values in format (datetime, lat, long, value)
        '''
        if history is not None:
            self.batch_add_points(history)
        self.__history = {}

    def batch_add_points(self, history: List[Tuple[datetime, float, float, float]]) -> None:
        '''
        Add more than one value

        :param history: list of historic values in format (datetime, lat, long, value)
        '''
        for date_time, lat, long, value in history:
            self.add_point(date_time, lat, long, value)

    def add_point(self, date_time: datetime, lat: float, long: float, value: float) -> None:
        '''
        Add a single point

        :param date_time: datetime
        :param lat:  latitude
        :param long:  longitude
        :param value: value
        :return_
        '''
        key = self.__get_key(lat, long)
        if key not in self.__history:
            self.__history[key] = []
        self.__history[key].append((date_time, value))
        self.__history[key].sort()

    def remove_point(self, date_time: datetime, lat: float, long: float) -> None:
        '''
        Remove a single point. If more than one value is stored for that location with same datetime, all will be deleted.
        :param date_time: datetime
        :param lat: latitude
        :param long: longitude
        :return:
        '''
        key = self.__get_key(lat, long)
        for saved_date_time, value in self.__history[key]:
            if saved_date_time > date_time:
                return  # Entries are ordered
            if saved_date_time == date_time:
                self.__history[key].remove((date_time, value))

    def remove_older_than(self, date_time: datetime, lat: float, long: float) -> None:
        '''
        Remove all points of that location that are older than datetime

        :param date_time: datetime
        :param lat: latitude
        :param long: longitude
        :return:
        '''
        key = self.__get_key(lat, long)
        for saved_date_time, value in self.__history[key]:
            if saved_date_time > date_time:
                return  # Entries are ordered
            self.__history[key].remove((saved_date_time, value))

    def get_value(self, date_time: datetime, lat: float, long: float) -> Optional[float]:
        '''
        Get value of a point at specific datetime

        :param date_time: datetime
        :param lat: latitude
        :param long: longitude
        :return:
        '''
        key = self.__get_key(lat, long)
        for saved_date_time, value in self.__history[key]:
            if saved_date_time == date_time:
                return value
        return None

    @staticmethod
    def __get_key(lat: float, long: float) -> str:
        '''
        Internal method to create a key from two floats

        :param lat: latitude
        :param long: longitude
        :return: a unique key
        '''
        return str(lat) + str(long)
