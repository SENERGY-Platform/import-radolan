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

def remove_prefix(text: str, prefix: str) -> str:
    '''
    Removes a prefix from a str. If prefix is not part of the text, it will be returned unmodified.

    :param text:
    :param prefix:
    :return: text without prefix
    '''
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def remove_suffix(text: str, suffix: str) -> str:
    '''
    Removes a suffix from a str. If suffix is not part of the text, it will be returned unmodified.

    :param text:
    :param suffix:
    :return: text without suffix
    '''
    if text.endswith(suffix):
        return text[:len(text) - len(suffix)]
    return text
