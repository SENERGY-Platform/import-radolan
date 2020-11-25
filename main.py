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

import time

import schedule
from import_lib.import_lib import ImportLib, get_logger

from radolan_lib.radolan.Products import str_to_product
from radolan_lib.radolan.RadolanImport import RadolanImport

if __name__ == '__main__':

    lib = ImportLib()
    logger = get_logger(__name__)
    product = lib.get_config("PRODUCT", "SF")
    try:
        product = str_to_product(product)
    except ValueError as e:
        logger.error(e)
        logger.error("Can't run with this product name. Exiting!")
        quit(1)

    radolan_import = RadolanImport(lib, product=product)

    state, _ = lib.get_last_published_datetime()
    if state is None:
        logger.info("Import is starting fresh")
    else:
        logger.info("Import is continuing previous import")

    import_years = lib.get_config("IMPORT_YEARS", [])
    for year in import_years:
        if state is not None and state.year > year:
            logger.info("Not reimporting data from " + str(year))
            continue
        elif state is not None and state.year == year:
            logger.info("Partially importing data from " + str(year))
            radolan_import.import_from_year(year, state)
        else:
            logger.info("Importing full data from " + str(year))
            radolan_import.import_from_year(year)

    radolan_import.import_most_recent()

    schedule.every().hour.at(":45").do(radolan_import.import_most_recent)

    while True:
        schedule.run_pending()
        time.sleep(1)
