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
import logging
import os
import time

import schedule
from confluent_kafka import Producer

from lib.radolan.sf.SFImport import SFImport

logging.basicConfig()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "import-radolan-sf")
CONFIG = json.loads(
    os.getenv("CONFIG",
              '{"EPSG": 4326, "BBOXES": [[12.35, 51.3, 12.4, 51.35],[9.88, 51.5, 10, 51.56]], "IMPORT_YEARS": []}'))
IMPORT_ID = os.getenv("IMPORT_ID", "unknown")

if __name__ == '__main__':
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP})
    bboxes = None
    if "BBOXES" in CONFIG:
        bboxes = CONFIG["BBOXES"]

    sf_import = SFImport(producer=producer, topic=KAFKA_TOPIC, epsg=CONFIG["EPSG"], import_id=IMPORT_ID,
                         bboxes=bboxes)

    if "IMPORT_YEARS" in CONFIG and len(CONFIG["IMPORT_YEARS"]) > 0:
        for year in CONFIG["IMPORT_YEARS"]:
            sf_import.import_from_year(year)

    sf_import.import_most_recent()

    schedule.every().hour.at(":45").do(sf_import.import_most_recent)

    while True:
        schedule.run_pending()
        time.sleep(1)
