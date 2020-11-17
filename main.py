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
from import_lib.import_lib import ImportLib

from lib.radolan.sf.SFImport import SFImport

if __name__ == '__main__':

    lib = ImportLib()
    sf_import = SFImport(lib)

    import_years = lib.get_config("IMPORT_YEARS", [])
    for year in import_years:
        sf_import.import_from_year(year)

    sf_import.import_most_recent()

    schedule.every().hour.at(":45").do(sf_import.import_most_recent)

    while True:
        schedule.run_pending()
        time.sleep(1)
