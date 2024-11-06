import requests
from bs4 import BeautifulSoup
import sqlite3

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

def scrape_listings():
    base_url = "https://nigeriapropertycentre.com/for-sale"
    response = requests.get(base_url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Find all property listings
    listings = soup.find_all("div", itemtype="https://schema.org/ListItem")

    for listing in listings:
        property_url = listing.find("a", itemprop="url")["href"]
        if property_url.startswith("/"):
            property_url = f"https://nigeriapropertycentre.com{property_url}"
        property_title = listing.find("h3", itemprop="name").text.strip()
        print(f"Scraping details for: {property_title}")
        scrape_property_details(property_url)

def scrape_property_details(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Details section
    details_table = soup.find("table", class_="table table-bordered table-striped")
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

    # Extract specific details
    added_on_year = details.get("Added On", "Unknown").split()[-1] if "Added On" in details else "Unknown"
    property_type = details.get("Type", "Unknown")
    bedrooms = details.get("Bedrooms", "Unknown")
    bathrooms = details.get("Bathrooms", "Unknown")
    toilets = details.get("Toilets", "Unknown")
    parking_spaces = details.get("Parking Spaces", "Unknown")

    # Location
    address_element = soup.find("address")
    if address_element:
        location = address_element.text.strip().split(",")
        town = location[-2].strip() if len(location) > 1 else "Unknown"
        state = location[-1].strip() if len(location) > 0 else "Unknown"
    else:
        town, state = "Unknown", "Unknown"

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

# Create database and table
create_database()

# Start scraping
scrape_listings()
