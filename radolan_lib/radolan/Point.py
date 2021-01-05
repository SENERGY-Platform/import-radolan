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

from typing import Dict, Tuple


def get_message(pos_long: float, pos_lat: float, epsg: int, value: float,
                precision: float, unit: str) -> Dict:
    '''
    Uses a single  DWD Radolan SF point to create a message for import by ensuring the correct format
    :param pos_long: longitude position
    :param pos_lat:  latitude position
    :param epsg: EPSG projection code
    :param value: precipitation in mm/d
    :param precision: precision of the measurement
    :param unit: unit of measurement
    :return: An annotated message ready to be imported
    '''

    return {
        "value": value,
        "meta": {
            "projection": "EPSG:" + str(epsg),
            "unit": unit,
            "precision": precision,
            "lat": pos_lat,
            "long": pos_long,
        }
    }


def extract_message(msg: Dict) -> Tuple[float, float, float, str, float, str]:
    '''
    Extracts a message

    :param msg: the message
    :return: Tuple with (lat, long, value, warn_level, warn_event, unit, precision, projection)
    :except ValueError: If the message is not in correct format
    '''

    if "value" not in msg or "meta" not in msg or "projection" not in \
            msg["meta"] or "unit" not in msg["meta"] or "precision" not in msg["meta"] or "lat" not in \
            msg["meta"] or "long" not in msg["meta"]:
        raise ValueError

    meta = msg["meta"]
    return meta["lat"], meta["long"], msg["value"],  meta["unit"], meta[
        "precision"], meta["projection"]
