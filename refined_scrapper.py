import requests
from bs4 import BeautifulSoup
import sqlite3
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time
import logging
import os

# Configure logging
logging.basicConfig(filename='scraper.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_database():
    conn = sqlite3.connect('for_sale.db')
    cursor = conn.cursor()
    
    # Create table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            added_on_year TEXT,
            type TEXT,
            bedrooms TEXT,
            bathrooms TEXT,
            toilets TEXT,
            parking_spaces TEXT,
            town TEXT,
            state TEXT,
            price TEXT
        )
    ''')
    
    conn.commit()
    conn.close()

def insert_listing(added_on_year, property_type, bedrooms, bathrooms, toilets, parking_spaces, town, state, price):
    conn = sqlite3.connect('for_sale.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        INSERT INTO listings (added_on_year, type, bedrooms, bathrooms, toilets, parking_spaces, town, state, price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (added_on_year, property_type, bedrooms, bathrooms, toilets, parking_spaces, town, state, price))
    
    conn.commit()
    conn.close()

def get_total_pages(base_url, session):
    response = session.get(base_url)
    soup = BeautifulSoup(response.content, "html.parser")

    pagination = soup.select("ul.pagination li a")
    total_pages = int(pagination[-2].text) if pagination else 1
    return total_pages

def get_last_scraped_page(filename="last_page.txt"):
    if os.path.exists(filename):
        with open(filename, "r") as file:
            last_page = file.read().strip()
            return int(last_page) if last_page.isdigit() else 1
    return 1

def save_last_scraped_page(page_number, filename="last_page.txt"):
    with open(filename, "w") as file:
        file.write(str(page_number))

def format_time(seconds):
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"

def scrape_listings(base_url, session):
    total_pages = get_total_pages(base_url, session)
    start_page = get_last_scraped_page()
    
    start_time = time.time()
    page_durations = []

    for page_number in range(start_page, total_pages + 1):
        try:
            page_start_time = time.time()
            
            print(f"Scraping page {page_number} of {total_pages}")
            logging.info(f"Scraping page {page_number} of {total_pages}")
            url = f"{base_url}?page={page_number}"
            response = session.get(url)
            soup = BeautifulSoup(response.content, "html.parser")

            # Find all property listings
            listings = soup.find_all("div", itemtype="https://schema.org/ListItem")
            if not listings:
                break

            for listing in listings:
                property_url = listing.find("a", itemprop="url")["href"]
                if property_url.startswith("/"):
                    property_url = f"https://nigeriapropertycentre.com{property_url}"
                scrape_property_details(property_url, session)
            
            save_last_scraped_page(page_number)  # Save the last scraped page number
            time.sleep(1)  # Delay to avoid overwhelming the server
            
            page_duration = time.time() - page_start_time
            page_durations.append(page_duration)
            
            avg_duration_per_page = sum(page_durations) / len(page_durations)
            remaining_pages = total_pages - page_number
            estimated_time_left = remaining_pages * avg_duration_per_page
            
            print(f"Estimated time left: {format_time(estimated_time_left)}")
            logging.info(f"Estimated time left: {format_time(estimated_time_left)}")
            
        except Exception as e:
            logging.error(f"Error on page {page_number}: {e}")
            save_last_scraped_page(page_number)  # Save the last scraped page number in case of error

def scrape_property_details(url, session):
    try:
        response = session.get(url)
        soup = BeautifulSoup(response.content, "html.parser")

        # Location
        address_element = soup.find("address")
        if address_element:
            location = address_element.text.strip().split(",")
            town = location[-2].strip() if len(location) > 1 else "Unknown"
            state = location[-1].strip() if len(location) > 0 else "Unknown"
        else:
            town, state = "Unknown", "Unknown"

        # Filter by specified states
        allowed_states = ["Abuja"]
        if state not in allowed_states:
            return

        # Details section
        details_table = soup.find("table", class_="table table-bordered table-striped")
        if details_table:
            rows = details_table.find_all("tr")
            details = {}

            for row in rows:
                cols = row.find_all("td")
                for col in cols:
                    if "Property Ref:" in col.text or "Last Updated:" in col.text:
                        continue  # Skip these details
                    key_value = col.text.split(":")
                    if len(key_value) == 2:
                        key = key_value[0].strip()
                        value = key_value[1].strip()
                        details[key] = value
        else:
            details = {}

        # Extract specific details
        property_type = details.get("Type", "Unknown")
        if property_type not in ["Flat / Apartment", "House", "Detached Duplex", "Semi-Detached Duplex", "Terraced Duplex", "Detached Bungalow", "Semi-Detached Bungalow", "Terraced Bungalow", "Maisonette"]:
            return

        print(f"Scraping details for: {property_type}")
        logging.info(f"Scraping details for: {property_type}")

        added_on_year = details.get("Added On", "Unknown").split()[-1] if "Added On" in details else "Unknown"
        bedrooms = details.get("Bedrooms", "Unknown")
        bathrooms = details.get("Bathrooms", "Unknown")
        toilets = details.get("Toilets", "Unknown")
        parking_spaces = details.get("Parking Spaces", "Unknown")

        # Price
        price_element = soup.select_one("span.property-details-price")
        if price_element:
            price_currency = price_element.find("span", itemprop="priceCurrency").text.strip() if price_element.find("span", itemprop="priceCurrency") else ""
            price_amount = price_element.find("span", itemprop="price").text.strip() if price_element.find("span", itemprop="price") else ""
            price = f"{price_currency}{price_amount}" if price_currency and price_amount else "No price available"
        else:
            price = "No price available"

        # Insert listing into the database
        insert_listing(added_on_year, property_type, bedrooms, bathrooms, toilets, parking_spaces, town, state, price)
        print(f"Added On Year: {added_on_year}")
        print(f"Type: {property_type}")
        print(f"Bedrooms: {bedrooms}")
        print(f"Bathrooms: {bathrooms}")
        print(f"Toilets: {toilets}")
        print(f"Parking Spaces: {parking_spaces}")
        print(f"Location: Town - {town}, State - {state}")
        print(f"Price: {price}")
        print("-----\n")
        logging.info(f"Added On Year: {added_on_year}, Type: {property_type}, Bedrooms: {bedrooms}, Bathrooms: {bathrooms}, Toilets: {toilets}, Parking Spaces: {parking_spaces}, Location: Town - {town}, State - {state}, Price: {price}")

    except Exception as e:
        logging.error(f"Error scraping details for {url}: {e}")

# Create a requests session with retry strategy
session = requests.Session()
retry = Retry(total=5, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)

# Create database and table
create_database()

# Start scraping all pages
base_url = "https://nigeriapropertycentre.com/for-sale"
scrape_listings(base_url, session)
