import logging
import time
from datetime import datetime
from typing import List, Union


import requests
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from bs4 import BeautifulSoup
from requests import Session



class CategoryProcessor:
    def __init__(self, category, aws_conn_id, destination):
        self._category = category
        self._aws_conn_id = aws_conn_id
        self._destination = destination
        self._session = Session()
        self._log = logging.getLogger('airflow.task')


    def build(self):

        @task(task_id=f'extract_{self._category}')
        def extract() -> 'List[str]' :
            return self._get_links()

        @task(task_id=f'transform_{self._category}')
        def transform(link: 'str') -> 'List[str]':
            return self._download_and_parse_page(link)

        @task(task_id=f'load_{self._category}')
        def load(parsed_data: 'List[str]'):
            self._send_to_s3(parsed_data)

        links = extract()
        parsed_data = transform.expand(link=links)
        load(parsed_data)

    def _get_bs_nav(self: 'CategoryProcessor', page: 'str', page_number: 'int', query: 'str' = '?page=') -> 'BeautifulSoup':
        if page_number > 1:
            page = f'{page}{query}{page_number}'
        try:
            res = self._session.get(page, timeout=None)
        except requests.exceptions.ConnectionError:
            time.sleep(5)
            return self._get_bs_nav(page, page_number, query)

        return BeautifulSoup(res.text, 'lxml')

    def _parse(self, html: 'str') -> 'List':
        soup = BeautifulSoup(html, 'lxml')
        sizes_list = []
        breadcrumbs_list = []

        base_info = soup.find('div', {'itemtype': 'http://schema.org/Offer'})
        name = soup.find('h1', {'class': 'item_name'}).text
        price = base_info.find('div', {'class': 'b-price'}).text
        old_price = base_info.find('div', {'class': 'b-old-price'})

        if old_price is not None:
            old_price = old_price.text
            price, old_price = old_price, price

        sizes = base_info.find('ul', {'class': 'b-sizes'}).find_all('li')
        for size in sizes:
            sizes_list.append(size.text.strip())

        props = base_info.find(
            'div', {'class': 'product-props'},
        ).find_all(
            'div',
            {'class': 'product-props__item'},
        )
        article, age, gender, season, color, collection, brand = None, None, None, None, None, None, None
        for prop in props:
            prop_name = prop.find('b').text.strip()
            if prop_name == 'Артикул:':
                article = self._replace_strip(prop, 'Артикул:')
            elif prop_name == 'Возраст:':
                age = ';'.join(map(lambda x: x.text, prop.find_all('a')))
            elif prop_name == 'Пол:':
                gender = self._replace_strip(prop, 'Пол:')
            elif prop_name == 'Сезон:':
                season = self._replace_strip(prop, 'Сезон:')
            elif prop_name == 'Цвет:':
                color = self._replace_strip(prop, 'Цвет:')
            elif prop_name == 'Коллекция:':
                collection = self._replace_strip(prop, 'Коллекция:')
            elif prop_name == 'Бренд:':
                brand = self._replace_strip(prop, 'Бренд:')
        description = soup.find('p', {'itemprop': 'description'}).text.strip()

        breadcrumbs = soup.find('div', {'class': 'b-bread'}).find_all('a', {'itemprop': 'item'})
        for breadcrumb in breadcrumbs:
            breadcrumbs_list.append(breadcrumb.text.strip())

        img = soup.find('div', {'class': 'big-pic'}).find('a', {'class': 'i-zoom'})
        if img is not None:
            img = f'https://kerry.su{img["href"]}'

        return [
            article,
            name,
            color,
            ';'.join(sizes_list),
            price,
            old_price,
            '/'.join(breadcrumbs_list),
            description,
            age,
            gender,
            season,
            collection,
            brand,
            img,
        ]

    def _get_links(self) -> 'List[str]':
        self._log.info('Start Extract Kerry, category: %s.', self._category)
        soup = self._get_bs_nav(f'https://kerry.su/{self._category}/', 1, '?PAGEN_3=')
        catalog = soup.find('div', {'class': 'b-hits'}).find_all('li', {'class': 'b-item'})

        return list(
            map(
                lambda link: f'https://kerry.su/{link.find("a")["href"]}',
                catalog
            )
        )


    def _download_and_parse_page(self, link: 'str') -> 'Union[List[str], None]':
        self._log.info('Processing %s', link)

        response = self._session.get(link, timeout=30, allow_redirects=False)

        try:
            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            self._log.error('Error downloading %s: %s', link, exc)
            return None

        result = self._parse(response.text)
        result.append(link)

        return result

    def _send_to_s3(self, parsed_data: 'List[str]'):
        self._log.info('Load result: %d for category: %s', len(parsed_data), self._category)

        date_str = str(datetime.now())[:19].replace(':', '.').replace(' ', '_')

        filename = f'{self._category}.{date_str}.csv'

        filtered_data = filter(lambda item: item is not None, parsed_data)

        with open(f'/tmp/{filename}', 'w', encoding='utf-8') as file:
            for item in filtered_data:
                file.write(f'{item}\n')

        hook = S3Hook(aws_conn_id=self._aws_conn_id)

        hook.load_file(
            filename=f'/tmp/{filename}',
            key=filename,
            replace=True,
            bucket_name=self._destination
        )

        self._log.info('Finish Extract Kerry, category: %s', self._category)

    @staticmethod
    def _replace_strip(el, text):
        return el.text.replace(text, '').strip()
