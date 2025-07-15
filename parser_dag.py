from airflow.decorators import task, dag
import pendulum
import requests
from requests import Session
import time
from bs4 import BeautifulSoup
import logging

task_logger = logging.getLogger('airflow.task')

_session = Session()


def _get_last_page(self, soup: 'BeautifulSoup'):
    last_page = soup.find(
        'div', {'class': 'pagination-wrapper'},
    ).find_all(
        'a', {'class': 'pagination-wrapper__pagination-list-item'},
    )[-1].text

    self._last_page = int(last_page)


def get_bs_nav(page: 'str', page_number: 'int', query: 'str' = '?page=') -> 'BeautifulSoup':
    if page_number > 1:
        page = f'{page}{query}{page_number}'
    try:
        res = _session.get(page, timeout=None)
    except requests.exceptions.ConnectionError as e:
        time.sleep(5)
        return get_bs_nav(page, page_number, query)

    return BeautifulSoup(res.text, 'lxml')

def _replace_strip(el, text):
    return el.text.replace(text, '').strip()


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['example'],
)
def parser_dag():

    @task
    def extract():
        task_logger.info('Start Extract Kerry')

        soup = get_bs_nav('https://kerry.su/devochki/', 1, '?PAGEN_3=')

        catalog = soup.find('div', {'class': 'b-hits'}).find_all('li', {'class': 'b-item'})

        links = []
        for item in catalog:
            link = item.find('a')
            item_link = f'https://kerry.su{link["href"]}'
            links.append(item_link)
        return links

    @task
    def download_page(link: 'str') -> 'dict|None':
        try:
            task_logger.info('Processing %s', link)
            response = requests.get(link, timeout=30, allow_redirects=False)
            response.raise_for_status()
            return {'link':link, 'html': response.text}
        except Exception as e:
            task_logger.error(f'Error downloading {link}: {str(e)}')
            return {'link':link, 'html': None}

    @task
    def transform(soups, **kwargs):
        task_logger.info('Start Transform Kerry')
        task_logger.info('Received %d pages', len(soups))


        valid_soups = []

        for soup in soups:
            if soup['html'] is not None:
                valid_soups.append({'link' : soup['link'], 'soup' : BeautifulSoup(soup['html'], 'lxml')})

        result = []

        for item in valid_soups:
            soup = item['soup']
            task_logger.info(f'Processing page: %s', soup.title.text)

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
                    article = _replace_strip(prop, 'Артикул:')
                elif prop_name == 'Возраст:':
                    age = ';'.join(map(lambda x: x.text, prop.find_all('a')))
                elif prop_name == 'Пол:':
                    gender = _replace_strip(prop, 'Пол:')
                elif prop_name == 'Сезон:':
                    season = _replace_strip(prop, 'Сезон:')
                elif prop_name == 'Цвет:':
                    color = _replace_strip(prop, 'Цвет:')
                elif prop_name == 'Коллекция:':
                    collection = _replace_strip(prop, 'Коллекция:')
                elif prop_name == 'Бренд:':
                    brand = _replace_strip(prop, 'Бренд:')
            description = soup.find('p', {'itemprop': 'description'}).text.strip()

            breadcrumbs = soup.find('div', {'class': 'b-bread'}).find_all('a', {'itemprop': 'item'})
            for breadcrumb in breadcrumbs:
                breadcrumbs_list.append(breadcrumb.text.strip())

            img = soup.find('div', {'class': 'big-pic'}).find('a', {'class': 'i-zoom'})
            if img is not None:
                img = f'https://kerry.su{img["href"]}'

            result.append([
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
                item['link'],
            ])
        task_logger.info(f'Processed %d pages', len(result))
        return result

    @task
    def load(transform_result, **kwargs):
        task_logger.info('Load result: %d', len(transform_result))

    links = extract()
    pages = download_page.expand(link=links)
    transform_result = transform(pages)
    load(transform_result)

dag = parser_dag()
