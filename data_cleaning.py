import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns

# Create a connection to the SQLite database
db_path = 'for_sale.db'
engine = create_engine(f'sqlite:///{db_path}')

# Load the data into a pandas DataFrame
df = pd.read_sql('SELECT * FROM listings', engine)

# Identify missing data
print(df.isnull().sum())

# Handle missing data
df['parking_spaces'] = df['parking_spaces'].replace('Unknown', df['parking_spaces'].mode()[0])
df['bathrooms'] = df['bathrooms'].replace('Unknown', df['bathrooms'].mode()[0])
df['toilets'] = df['toilets'].replace('Unknown', df['toilets'].mode()[0])

# Convert data types
df['bedrooms'] = pd.to_numeric(df['bedrooms'], errors='coerce')
df['bathrooms'] = pd.to_numeric(df['bathrooms'], errors='coerce')
df['toilets'] = pd.to_numeric(df['toilets'], errors='coerce')
df['parking_spaces'] = pd.to_numeric(df['parking_spaces'], errors='coerce')

# Save the cleaned data back to the database
df.to_sql('cleaned_listings', engine, if_exists='replace', index=False)

# Plot the distribution of property types
plt.figure(figsize=(10, 6))
sns.countplot(x='type', data=df)
plt.title('Distribution of Property Types')
plt.xticks(rotation=45)
plt.show()

# Plot the distribution of prices
plt.figure(figsize=(10, 6))
sns.histplot(df['price'], bins=50, kde=True)
plt.title('Distribution of Property Prices')
plt.show()
