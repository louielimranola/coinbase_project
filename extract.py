import requests
import pandas as pd
import psycopg2
import json
from datetime import datetime

# Set up PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",        # Assuming PostgreSQL is running locally
    database="coinbase_db",  # Your database name
    user="airflow",         # Your PostgreSQL username
    password="airflow" # Your PostgreSQL password
)

cursor = conn.cursor()

# Fetch Bitcoin price data
prices_url = 'https://api.coinbase.com/v2/prices/BTC-USD/buy?date=2021-01-01'
response_prices = requests.get(prices_url)
prices = json.loads(response_prices.text)

# Extract values from the response
amount = float(prices['data']['amount'])
base = prices['data']['base']
currency = prices['data']['currency']

# Insert Bitcoin price data into PostgreSQL
cursor.execute(
    "INSERT INTO btc_prices (amount, base, currency) VALUES (%s, %s, %s)",
    (amount, base, currency)
)
conn.commit()  # Commit the transaction

print("Data inserted successfully!")

# Close the connection
cursor.close()
conn.close()
