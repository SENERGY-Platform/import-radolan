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

import json
import datetime
from rfc3339 import rfc3339

factor = 0.1


def get_message(pos_long: float, pos_lat: float, epsg: int, datetime: datetime, val_tenth_mm_d: float,
                precision: float, import_id: str) -> str:
    return json.dumps({
        "import_id": import_id,
        "time": rfc3339(datetime, utc=True, use_system_timezone=False),
        "value": round(val_tenth_mm_d * factor, 2),
        "meta": {
            "projection": "EPSG:" + str(epsg),
            "unit": "mm/d",
            "precision": round(precision * factor, 2),
            "lat": pos_lat,
            "long": pos_long,
        }
    })
