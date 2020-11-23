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

from typing import Dict

factor = 0.1


def get_message(pos_long: float, pos_lat: float, epsg: int, val_tenth_mm_d: float,
                precision: float) -> Dict:
    '''
    Uses a single  DWD Radolan SF point to create a message for import by ensuring the correct format and adding annotations
    Warning levels are annotated according to https://www.dwd.de/DE/wetter/warnungen_aktuell/kriterien/warnkriterien.html

    :param pos_long: longitude position
    :param pos_lat:  latitude position
    :param epsg: EPSG projection code
    :param val_tenth_mm_d: precipitation in 1/10 mm/d
    :param precision: precision of the measurement
    :return: An annotated message ready to be imported
    '''

    value = round(val_tenth_mm_d * factor, 2)
    warn_level = 0
    warn_event = ""
    if 30 <= value <= 50:
        warn_level = 2
        warn_event = "Dauerregen"
    elif 50 < value <= 80:
        warn_level = 3
        warn_event = "Ergiebiger Dauerregen"
    elif value > 80:
        warn_level = 4
        warn_event = "Extrem ergiebiger Dauerregen"

    return {
        "value": value,
        "warn_level": warn_level,
        "warn_event": warn_event,
        "meta": {
            "projection": "EPSG:" + str(epsg),
            "unit": "mm/d",
            "precision": round(precision * factor, 2),
            "lat": pos_lat,
            "long": pos_long,
        }
    }
