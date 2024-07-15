from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
import requests
from bs4 import BeautifulSoup
import sqlite3
import json
import os
import redis
import time

app = FastAPI()

DATABASE = 'scraped_data.db'
IMAGE_DIR = 'images'
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

# Initialize Redis
cache = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

# OAuth2PasswordBearer for authentication
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


# Database setup
def init_db():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_title TEXT,
            product_price REAL,
            path_to_image TEXT
        )
    ''')
    conn.commit()
    conn.close()


init_db()


class ScrapeSettings(BaseModel):
    limit_pages: int = Query(default=5, description="Limit the number of pages to scrape")
    proxy: str = Query(default=None, description="Proxy string to use for scraping", example="http://proxyserver:port")


class Scraper:
    def __init__(self, settings: ScrapeSettings):
        self.base_url = "https://dentalstall.com/shop/"
        self.settings = settings
        self.products = []
        if not os.path.exists(IMAGE_DIR):
            os.makedirs(IMAGE_DIR)

    def scrape(self):
        page = 1
        while page <= self.settings.limit_pages:
            url = f"{self.base_url}page/{page}/"
            proxies = {"http": self.settings.proxy, "https": self.settings.proxy} if self.settings.proxy else None

            for _ in range(3):  # Retry mechanism
                try:
                    response = requests.get(url, proxies=proxies)
                    print(response)
                    if response.status_code == 200:
                        break
                except requests.RequestException:
                    time.sleep(3)
            else:
                continue

            soup = BeautifulSoup(response.content, 'html.parser')
            product_cards = soup.find_all('div', class_='mf-product-details')

            for card in product_cards:
                print(card)
                print("***********")
                name_tag = card.find('h2', class_='woo-loop-product__title')
                name = name_tag.text.strip() if name_tag else 'No name found'

                price_tag = card.find('span', class_='price')
                if price_tag:
                    current_price_tag = price_tag.find('ins')
                    if current_price_tag:
                        current_price = current_price_tag.text.strip().replace('₹', '').replace(',', '')
                    else:
                        current_price = price_tag.text.strip().replace('₹', '').replace(',', '')
                else:
                    current_price = '0.0'

                try:
                    current_price = float(current_price)
                except ValueError:
                    current_price = '0.0'

                img_tag = card.find('img', class_='mf-product-thumbnail')
                image_url = img_tag['src'] if img_tag else None
                image_path = self.save_image(image_url, name) if image_url else 'No image found'
                product = {
                    "product_title": name,
                    "product_price": current_price,
                    "path_to_image": image_path
                }
                print(product)

                # Cache check
                cached_price = cache.get(name)
                if cached_price is None or float(cached_price) != product["product_price"]:
                    self.products.append(product)
                    self.store_in_db(product)
                    cache.set(name, product["product_price"])

            page += 1

    def save_image(self, url, name):
        response = requests.get(url)
        image_name = f"{name.replace(' ', '_').lower()}.jpg"
        image_path = os.path.join(IMAGE_DIR, image_name)
        with open(image_path, 'wb') as file:
            file.write(response.content)
        return image_path

    def store_in_db(self, product):
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO products (product_title, product_price, path_to_image)
            VALUES (?, ?, ?)
        ''', (product['product_title'], product['product_price'], product['path_to_image']))
        conn.commit()
        conn.close()

    def save_to_json(self):
        with open('scraped_data.json', 'w') as json_file:
            json.dump(self.products, json_file, indent=4)


class Notification:
    def notify(self, message: str):
        print(message)


class ScrapingSession:
    def __init__(self, settings: ScrapeSettings):
        self.scraper = Scraper(settings)
        self.notifier = Notification()

    def run(self):
        self.scraper.scrape()
        self.scraper.save_to_json()
        message = f"Scraped {len(self.scraper.products)} products."
        self.notifier.notify(message)


@app.post("/scrape")
def scrape_website(settings: ScrapeSettings, token: str = Depends(oauth2_scheme)):
    if token != "your_static_token":  # Simple token check
        raise HTTPException(status_code=401, detail="Unauthorized")

    session = ScrapingSession(settings)
    session.run()
    return {"status": "success", "data": session.scraper.products}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
