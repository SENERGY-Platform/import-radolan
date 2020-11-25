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

from typing import Type


class Product:
    pass


class SF(Product):
    pass


class RW(Product):
    pass


def is_known_product(product: Type[Product]) -> bool:
    '''
    Checks if a supplied product type is one of the known products

    :param product: product type
    :return: True, if supplied product type is known
    '''
    return product == SF or product == RW


products = {
    "SF": SF,
    "RW": RW,
}


def str_to_product(string: str) -> Type[Product]:
    '''
    Translates a product string into a product type

    :param string: product as string
    :return: product as type
    :except ValueError: if string does not match a product
    '''
    string = string.upper()
    if string not in products:
        raise ValueError("Unknown product")
    return products[string]
