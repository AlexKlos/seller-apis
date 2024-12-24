import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получает список товаров магазина озон.

    Args:
        last_id (str): Индекс последнего обработанного товара
        client_id (str): Идентефикатор пользователя
        seller_token (str): Токен продавца озон

    Returns:
        dict: Словарь со списком товаров

    Raises:
        requests.exceptions.HTTPError: Если сервер вернёт HTTP-ошибку.
        requests.exceptions.RequestException: Если возникнет ошибка при выполнении запроса.
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получает артикулы товаров магазина Озон.

    Args:
        client_id (str): Идентефикатор пользователя
        seller_token (str): Токен продавца озон

    Returns:
        list: Список артикулов товаров
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновляет цены товаров.

    Загружает обновлённые цены на товарыв магазин Озон.

    Args:
        prices (list): Список содержащий цены товаров
        client_id (str): Идентефикатор пользователя
        seller_token (str): Токен продавца озон

    Returns:
        dict: Возвращает ответ о выполнении операции

    Raises:
        requests.exceptions.HTTPError: Если сервер вернёт HTTP-ошибку.
        requests.exceptions.RequestException: Если возникнет ошибка при выполнении запроса.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновляет остатки товаров.

    Загружает обновлённые товарные остатки в магазин Озон.

    Args:
        stocks (list): Список содержащий остатки товаров
        client_id (str): Идентефикатор пользователя
        seller_token (str): Токен продавца Озон

    Returns:
        dict: Возвращает ответ о выполнении операции

    Raises:
        requests.exceptions.HTTPError: Если сервер вернёт HTTP-ошибку.
        requests.exceptions.RequestException: Если возникнет ошибка при 
            выполнении запроса.
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачивает товарные остатки с сайта casio.

    Скачивает архив с остатками часов, распаковывает и извлекает данные 
    из Excel-файла, удаляет файл после обработки.
    
    Returns:
        list: Список остатков часов. Каждый элемент — это словарь с данными 
            одной строки Excel-файла.

    Raises:
        requests.exceptions.RequestException: Если загрузка файла завершилась с ошибкой.
        zipfile.BadZipFile: Если архив повреждён.
        pandas.errors.ExcelFileError: Если файл Excel не удаётся прочитать.
        FileNotFoundError: Если файл `ostatki.xls` не найден.
    """
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создаёт список с товарными остатками.

    Если для артикула в списке watch_remnants значение остатка >10, то 
    присваивает ему значение 100.
    Если для артикула в списке watch_remnants значение остатка 1, то 
    присваивает ему значение 0.
    В остальных случаях присваивает значение остатка соответствующее 
    значению из watch_remnants.
    Если значение артикула из offer_ids отсутствует в списке watch_remnants, 
    то значение остатка для него = 0.

    Args:
        watch_remnants (list): Список остатков часов. Каждый элемент — это 
            словарь с данными по одному артикулу
        offer_ids (list): Список артикулов товаров

    Returns:
        list: Список остатков товаров, каждый элемент в 
            формате {"offer_id": <артикул>, "stock": <остаток>}

    Examples:
        >>> watch_remnants = [
                {"Код": "123", "Количество": ">10"},
                {"Код": "124", "Количество": "1"},
                {"Код": "125", "Количество": "5"},
            ]
        >>> offer_ids = ["123", "124", "126"]
        >>> create_stocks(watch_remnants, offer_ids)
        [
            {'offer_id': '123', 'stock': 100},
            {'offer_id': '124', 'stock': 0},
            {'offer_id': '125', 'stock': 5},
            {'offer_id': '126', 'stock': 0}
        ]
    """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создаёт список с ценами.

    Args:
        watch_remnants (list): Список остатков часов. Каждый элемент — это 
            словарь с данными по одному артикулу
        offer_ids (list): Список артикулов товаров

    Returns:
        list: Список цен. Каждый элемент — словарь с полями:
            - `auto_action_enabled` (str): Статус автонастройки цены 
            (заглушка: `UNKNOWN`).
            - `currency_code` (str): Код валюты (например, `RUB`).
            - `offer_id` (str): Идентификатор товара.
            - `old_price` (str): Старая цена (заглушка: `0`).
            - `price` (str): Цена товара.

    Examples:
        >>> watch_remnants = [
                {"Код": "123", "Цена": "5000.00"},
                {"Код": "124", "Цена": "7000.50"},
                {"Код": "125", "Цена": "1200.00"},
            ]
        >>> offer_ids = ["123", "124"]
        >>> create_prices(watch_remnants, offer_ids)
        [
            {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "123",
                "old_price": "0",
                "price": 5000.0
            },
            {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": "124",
                "old_price": "0",
                "price": 7000.5
            }
        ]
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразует цену в нужный формат.

    Удаляет из строки все символы, кроме цифр и отбрасывает все символы
    после '.' включительно.

    Args:
        price (str): Неформатированная строка с ценой

    Returns:
        str: Форматированная строка с ценой

    Examples:
        >>> price_conversion('123.45 USD')
        123
        >>> price_conversion('a1b2c3')
        123

    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделяет список lst на части по n элементов.

    Функция является генератором и возвращает части списка (субсписки) по одной.

    Args:
        lst (list): Исходный список, который нужно разделить
        n (int): Количество элементов в каждой части (субсписке)

    Yields:
        list: Часть списка, содержащая до `n` элементов

    Examples:
        >>> list(divide([1, 2, 3, 4, 5, 6, 7], 3))
        [[1, 2, 3], [4, 5, 6], [7]]

        >>> for part in divide([1, 2, 3, 4, 5], 2):
        ...     print(part)
        [1, 2]
        [3, 4]
        [5]
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Обновляет цены товаров в магазине Озон.

    Args:
        watch_remnants (list): Список остатков товаров. Каждый элемент — это 
            словарь с данными по одному артикулу
        client_id (str): Идентефикатор пользователя
        seller_token (str): Токен продавца Озон

    Returns:
        list: Список цен. Каждый элемент — словарь с полями:
            - `auto_action_enabled` (str): Статус автонастройки цены 
            (заглушка: `UNKNOWN`).
            - `currency_code` (str): Код валюты (например, `RUB`).
            - `offer_id` (str): Идентификатор товара.
            - `old_price` (str): Старая цена (заглушка: `0`).
            - `price` (str): Цена товара.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Обновляет остатки товара в магазине Озон

    Args:
        watch_remnants (list): Список остатков товаров. Каждый элемент — это
            словарь с данными по одному товару
        client_id (str): Идентефикатор пользователя
        seller_token (str): Токен продавца Озон

    Returns:
        tuple: 
            - list: Список товаров с ненулевыми остатками.
            - list: Полный список обработанных остатков.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
