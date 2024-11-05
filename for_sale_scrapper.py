import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import sqlite3
import re
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define database
conn = sqlite3.connect('for_sale_data.db')
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS properties_for_sale 
             (house_type TEXT, price TEXT, bedrooms INTEGER, bathrooms INTEGER, 
              parking_space INTEGER, toilets INTEGER, town TEXT, state TEXT, features TEXT)''')

# Function to scrape a page
def scrape_page(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    response = requests.get(url, headers=headers, timeout=10)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Check for the presence of property listings
    properties = soup.find_all('div', class_='wp-block property list')
    if not properties:
        logging.warning(f"No properties found on page: {url}")
        return False
    
    for prop in properties:
        try:
            house_type_elem = prop.find('h4', class_='content-title')
            price_parts = prop.find_all('span', class_='price')
            location_elem = prop.find('address', class_='voffset-bottom-10')
            features_elem = prop.find('ul', class_='aux-info')
            
            # Check if elements are found before accessing text
            house_type = house_type_elem.text.strip().split(' for sale')[0] if house_type_elem else 'N/A'
            price = ' '.join([part.text.strip() for part in price_parts]) if price_parts else 'N/A'
            location = location_elem.text.strip() if location_elem else 'N/A'
            
            # Extract features
            features = ', '.join([feature.text.strip() for feature in features_elem.find_all('li')]) if features_elem else 'N/A'
            
            # Split location into town and state
            location_parts = location.split(', ')
            town = location_parts[-2] if len(location_parts) >= 2 else 'N/A'
            state = location_parts[-1] if len(location_parts) >= 2 else 'N/A'
            
            # Extract number of bedrooms, bathrooms, parking spaces, and toilets
            bedrooms = int(re.search(r'(\d+)\s*Bedrooms', features).group(1)) if 'Bedrooms' in features else 0
            bathrooms = int(re.search(r'(\d+)\s*Bathrooms', features).group(1)) if 'Bathrooms' in features else 0
            parking_spaces = int(re.search(r'(\d+)\s*Parking Spaces', features).group(1)) if 'Parking Spaces' in features else 0
            toilets = int(re.search(r'(\d+)\s*Toilets', features).group(1)) if 'Toilets' in features else 0
            
            # Logging output
            logging.info(f"Scraped property - House Type: {house_type}, Price: {price}, Location: {location}, Features: {features}, Bedrooms: {bedrooms}, Bathrooms: {bathrooms}, Parking Spaces: {parking_spaces}, Toilets: {toilets}")
            
            # Save to database
            c.execute("""INSERT INTO properties_for_sale 
                         (house_type, price, bedrooms, bathrooms, parking_space, toilets, town, state, features) 
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                      (house_type, price, bedrooms, bathrooms, parking_spaces, toilets, town, state, features))
        except (AttributeError, IndexError, ValueError) as e:
            logging.error(f"Error scraping property: {e}")
    return True

# Total number of pages to scrape
pages_to_scrape = 5

# Loop through each page and scrape data
for page in tqdm(range(1, pages_to_scrape + 1), desc="Scraping Pages"):
    url = f'https://nigeriapropertycentre.com/for-sale?page={page}'
    if scrape_page(url):
        logging.info(f"Page {page} scraped successfully")
    else:
        logging.warning(f"Page {page} could not be scraped")

# Commit and close database
conn.commit()
conn.close()
