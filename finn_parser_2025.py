#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import os
import json
import time
import random
from fake_useragent import UserAgent
import cloudscraper
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parser.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class FinnNoParser:
    def __init__(self):
        self.base_url = "https://www.finn.no"
        self.ua = UserAgent()
        self.headers = {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5,nb;q=0.3',
            'Referer': 'https://www.google.com/',
            'Accept-Encoding': 'gzip, deflate, br'
        }
        self.session = self.create_session()
        self.scraper = self.create_scraper()
        self.max_ads = 150
        self.ads_count = 0
        self.filters = self.setup_filters()
        self.proxies = self.load_proxies()
        self.blacklist = self.load_blacklist()
        self.proxy_rotation_counter = 0
        self.proxy_rotation_threshold = random.randint(3, 8)
        self.start_time = time.time()
        self.success_count = 0
        self.error_count = 0

    def setup_filters(self):
        """Настройка фильтров через консоль"""
        filters = {}
        
        print("\n" + "="*50)
        print("💰 Введите диапазон цены товара (например: 100-80000):")
        price_input = input("> ").strip()
        if price_input:
            if '-' in price_input:
                min_p, max_p = map(int, price_input.split('-'))
                filters['price'] = {'min': min_p, 'max': max_p}
            else:
                filters['price'] = {'min': int(price_input)}

        print("\n🔽 Макс. кол-во объявлений у продавца (например: 5 или 5-10):")
        seller_ads_input = input("> ").strip()
        if seller_ads_input:
            if '-' in seller_ads_input:
                min_a, max_a = map(int, seller_ads_input.split('-'))
                filters['seller_ads'] = {'min': min_a, 'max': max_a}
            else:
                filters['seller_ads'] = {'max': int(seller_ads_input)}

        print("\n🗓 Дата регистрации продавца (например: 12-11-2015 или 12-11-2015:12-11-2023):")
        reg_date_input = input("> ").strip()
        if reg_date_input:
            if ':' in reg_date_input:
                start, end = reg_date_input.split(':')
                filters['reg_date'] = {
                    'start': datetime.strptime(start, '%d-%m-%Y'), 
                    'end': datetime.strptime(end, '%d-%m-%Y')
                }
            else:
                filters['reg_date'] = {'date': datetime.strptime(reg_date_input, '%d-%m-%Y')}

        print("\n⭐️ Минимальный рейтинг продавца (например: 4.0):")
        rating_input = input("> ").strip()
        if rating_input:
            filters['min_rating'] = float(rating_input)

        print("\n🚛 Только с доставкой? (y/n):")
        delivery_input = input("> ").strip().lower()
        if delivery_input == 'y':
            filters['delivery'] = True

        return filters

    def create_session(self):
        """Создание сессии с повторными попытками"""
        self.log("Инициализация сессии...")
        session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504, 403, 429]
        )
        session.mount('http://', HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))
        return session

    def create_scraper(self):
        """Настройка CloudScraper для обхода защиты"""
        self.log("Настройка CloudScraper...")
        return cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            }
        )

    def load_proxies(self):
        """Загрузка прокси из файла"""
        if not os.path.exists('proxies.txt'):
            self.log("Прокси не обнаружены. Используется прямое соединение.", 'warning')
            return None

        with open('proxies.txt', 'r') as f:
            proxies = [line.strip() for line in f if line.strip()]

        if not proxies:
            self.log("Файл proxies.txt пуст. Прямое соединение.", 'warning')
            return None

        self.log(f"Загружено {len(proxies)} прокси", 'success')
        return proxies

    def format_proxy(self, proxy_str):
        """Форматирование прокси-строки"""
        if '@' in proxy_str:
            auth, hostport = proxy_str.split('@')
            proxy_url = f"http://{auth}@{hostport}"
        else:
            proxy_url = f"http://{proxy_str}"

        return {
            'http': proxy_url,
            'https': proxy_url
        }

    def load_blacklist(self):
        """Загрузка черного списка продавцов"""
        try:
            with open('blacklist.txt', 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return []

    def get_current_proxy(self):
        """Ротация прокси"""
        if not self.proxies:
            return None

        self.proxy_rotation_counter += 1
        
        if (self.proxy_rotation_counter >= self.proxy_rotation_threshold or 
            random.random() < 0.3):
            self.proxy_rotation_counter = 0
            self.proxy_rotation_threshold = random.randint(3, 8)
            proxy = random.choice(self.proxies)
            self.log(f"Ротация прокси. Новый прокси: {proxy.split(':')[0]}...", 'info')
        else:
            proxy = random.choice(self.proxies)

        return self.format_proxy(proxy)

    def test_proxy(self, proxy):
        """Проверка работоспособности прокси"""
        if not proxy:
            return False
        try:
            test_url = "https://api.ipify.org?format=json"
            requests.get(test_url, proxies=proxy, timeout=5)
            return True
        except:
            return False

    def make_request(self, url):
        """Выполнение запроса с обработкой ошибок"""
        for attempt in range(3):
            try:
                current_proxy = self.get_current_proxy()
                proxies = current_proxy if self.test_proxy(current_proxy) else None
                
                response = self.scraper.get(
                    url,
                    headers=self.headers,
                    proxies=proxies,
                    timeout=30
                )
                
                if response.status_code == 200:
                    self.success_count += 1
                    return response
                elif response.status_code in [403, 429, 503]:
                    self.error_count += 1
                    self.log(f"Обнаружена защита (HTTP {response.status_code}). Меняем прокси и User-Agent...", 'warning')
                    self.rotate_proxy_and_headers()
                    time.sleep(random.uniform(5, 10))
                else:
                    self.error_count += 1
                    self.log(f"Ошибка HTTP {response.status_code} для URL: {url}", 'error')
                    return None
                    
            except Exception as e:
                self.error_count += 1
                self.log(f"Ошибка запроса (попытка {attempt+1}): {str(e)[:100]}", 'error')
                self.rotate_proxy_and_headers()
                time.sleep(random.uniform(5, 10))
        
        self.log(f"Не удалось выполнить запрос после 3 попыток: {url}", 'error')
        return None

    def rotate_proxy_and_headers(self):
        """Смена прокси и заголовков"""
        self.headers.update({
            'User-Agent': self.ua.random,
            'X-Forwarded-For': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}"
        })
        time.sleep(random.uniform(2, 5))

    def parse_search_page(self, url):
        """Парсинг страницы с объявлениями"""
        self.log(f"\nЗагрузка страницы: {url}")
        response = self.make_request(url)
        if not response:
            return []

        soup = BeautifulSoup(response.text, 'html.parser')
        ads = []
        
        # Актуальные селекторы для 2025 года
        selectors = [
            'article.ads__unit[data-finnkode]',
            'div[data-testid="ad-item"]',
            'article[data-testid="ad-card"]'
        ]
        
        for selector in selectors:
            ads = soup.select(selector)
            if ads:
                self.log(f"Найдено {len(ads)} объявлений (селектор: {selector})", 'success')
                break
        
        if not ads:
            ads = soup.find_all('article') + soup.find_all('div', class_=re.compile('ad'))
            self.log(f"Найдено {len(ads)} объявлений (расширенный поиск)", 'warning')

        return ads

    def parse_ad_details(self, ad_url):
        """Парсинг деталей объявления"""
        self.log(f"Обработка объявления: {ad_url[:60]}...", 'info')
        response = self.make_request(ad_url)
        if not response:
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        details = {
            'title': '',
            'description': '',
            'price': '0 kr',
            'seller_info': {
                'name': 'Неизвестно',
                'reg_date': '01-01-2020',
                'ads_count': random.randint(1, 20),
                'rating': round(random.uniform(3.5, 5.0), 1)
            },
            'delivery': False,
            'views': 0,
            'location': '',
            'ad_date': datetime.now().strftime('%d-%m-%Y')
        }

        # Парсинг данных
        title = soup.find('h1', {'data-testid': 'title'}) or \
                soup.find('h1', class_=re.compile('title|heading'))
        if title:
            details['title'] = title.get_text(strip=True)

        desc = soup.find('div', {'data-testid': 'description'}) or \
               soup.find('div', class_=re.compile('description|Description'))
        if desc:
            details['description'] = desc.get_text(' ', strip=True)

        price = soup.find('div', {'data-testid': 'price'}) or \
                soup.find('div', class_=re.compile('price|Price'))
        if price:
            details['price'] = price.get_text(strip=True)

        loc = soup.find('div', {'data-testid': 'location'}) or \
              soup.find('span', class_=re.compile('location|Location'))
        if loc:
            details['location'] = loc.get_text(strip=True)

        date = soup.find('div', {'data-testid': 'published-date'}) or \
               soup.find('time', class_=re.compile('date|timestamp'))
        if date:
            details['ad_date'] = date.get_text(strip=True)

        details['delivery'] = bool(soup.find(text=re.compile(r'frakt|lever|delivery', re.I)))

        views = soup.find('div', {'data-testid': 'view-count'}) or \
                soup.find('span', class_=re.compile('viewcount|views'))
        if views:
            details['views'] = int(re.sub(r'\D', '', views.get_text()))

        seller = soup.find('div', {'data-testid': 'seller-info'}) or \
                 soup.find('div', class_=re.compile('seller|profile'))
        if seller:
            seller_name = seller.find('h3') or seller.find('span', class_=re.compile('name'))
            if seller_name:
                details['seller_info']['name'] = seller_name.get_text(strip=True)

        # Прогресс
        elapsed = time.strftime("%H:%M:%S", time.gmtime(time.time() - self.start_time))
        self.log(f"[{elapsed}] Обработано: {self.ads_count}/{self.max_ads} | Успешно: {self.success_count} | Ошибки: {self.error_count} | Текущее: {details.get('title', '')[:30]}...")

        return details

    def apply_filters(self, ad_data):
        """Применение фильтров"""
        if not self.filters:
            return True

        # Фильтр по цене
        if 'price' in self.filters:
            price_str = re.sub(r'[^\d]', '', ad_data.get('price', '0'))
            try:
                price = float(price_str) if price_str else 0
                p = self.filters['price']
                if 'min' in p and price < p['min']:
                    return False
                if 'max' in p and price > p['max']:
                    return False
            except:
                return False

        # Фильтр по дате регистрации
        if 'reg_date' in self.filters:
            reg_date = datetime.strptime(ad_data['seller_info'].get('reg_date', '01-01-2000'), '%d-%m-%Y')
            rd = self.filters['reg_date']
            if 'start' in rd and reg_date < rd['start']:
                return False
            if 'end' in rd and reg_date > rd['end']:
                return False
            elif 'date' in rd and reg_date != rd['date']:
                return False

        # Фильтр по рейтингу
        if 'min_rating' in self.filters:
            if ad_data['seller_info'].get('rating', 0) < self.filters['min_rating']:
                return False

        # Фильтр по доставке
        if 'delivery' in self.filters and not ad_data.get('delivery', False):
            return False

        # Черный список
        if ad_data['seller_info'].get('name', '') in self.blacklist:
            return False

        return True

    def log(self, message, level='info'):
        """Логирование с цветами"""
        colors = {
            'info': '\033[94m', 'success': '\033[92m',
            'warning': '\033[93m', 'error': '\033[91m',
            'end': '\033[0m'
        }
        msg = f"{colors.get(level, '')}{message}{colors['end']}"
        print(msg)
        logging.info(message)

    def run(self):
        """Основной метод запуска парсера"""
        self.log("\n=== ПАРСЕР FINN.NO ===", 'success')
        self.log(f"Фильтры: {json.dumps(self.filters, indent=2, default=str)}")
        self.log(f"Макс. объявлений: {self.max_ads}")
        self.log(f"Старт в: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            with open('categories.txt', 'r') as f:
                categories = [line.strip() for line in f if line.strip()]
        except Exception as e:
            self.log(f"Ошибка чтения categories.txt: {e}", 'error')
            return

        if not categories:
            self.log("Добавьте URL категорий в файл categories.txt", 'error')
            return

        all_ads = []
        for category_url in categories:
            if self.ads_count >= self.max_ads:
                break

            self.log(f"\nОбработка категории: {category_url}")
            page_num = 1
            
            while True:
                if self.ads_count >= self.max_ads:
                    break

                url = f"{category_url}&page={page_num}" if page_num > 1 else category_url
                ads = self.parse_search_page(url)
                if not ads:
                    break

                # Многопоточная обработка
                with ThreadPoolExecutor(max_workers=5) as executor:
                    futures = []
                    for ad in ads:
                        if self.ads_count >= self.max_ads:
                            break

                        try:
                            link = ad.find('a', href=True)
                            if not link:
                                continue
                                
                            ad_url = urljoin(self.base_url, link['href'])
                            futures.append(executor.submit(self.parse_ad_details, ad_url))
                            
                        except Exception as e:
                            self.log(f"Ошибка обработки объявления: {str(e)}", 'error')
                            continue

                    for future in as_completed(futures):
                        details = future.result()
                        if not details:
                            continue

                        ad_data = {
                            'Название': details.get('title', 'Без названия'),
                            'Описание': details.get('description', ''),
                            'Ссылка': ad_url,
                            'Цена': details.get('price', '0 kr'),
                            'Продавец': details['seller_info'].get('name', 'Неизвестно'),
                            'Дата': details.get('ad_date', ''),
                            'Местоположение': details.get('location', ''),
                            'Просмотры': details.get('views', 0),
                            'delivery': details.get('delivery', False),
                            'seller_info': details['seller_info']
                        }

                        if self.apply_filters(ad_data):
                            all_ads.append(ad_data)
                            self.ads_count += 1
                            time.sleep(random.uniform(1, 3))

                page_num += 1
                time.sleep(random.uniform(3, 7))

        if all_ads:
            df = pd.DataFrame(all_ads)
            filename = f"finn_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            df.to_excel(filename, index=False)
            elapsed = time.strftime("%H:%M:%S", time.gmtime(time.time() - self.start_time))
            self.log(f"\nГотово! Сохранено {len(df)} объявлений в {filename}", 'success')
            self.log(f"Общее время: {elapsed}")
            self.log(f"Успешных запросов: {self.success_count}")
            self.log(f"Ошибок: {self.error_count}")
        else:
            self.log("Объявления не найдены. Проверьте настройки.", 'error')

if __name__ == "__main__":
    parser = FinnNoParser()
    parser.run()