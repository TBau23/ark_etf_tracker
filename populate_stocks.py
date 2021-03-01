from dotenv import load_dotenv
load_dotenv()
import os
import alpaca_trade_api as tradeapi
import psycopg2
import psycopg2.extras

connection = psycopg2.connect(host=os.getenv("DB_HOST"), database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"))

cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor) # by default this returns a list of tuples, can turn rows into dict entries



api = tradeapi.REST(os.getenv("API_KEY"), os.getenv("API_SECRET"), base_url=os.getenv("API_URL"))

assets = api.list_assets()

for asset in assets:
    print(f"Inserting stock {asset.symbol} : {asset.name}")
    cursor.execute("""
    INSERT INTO stock (name, symbol, exchange, is_etf)
    VALUES (%s, %s, %s, %s)
    """, (asset.name, asset.symbol, asset.exchange, False))

connection.commit() # ensures that postgres actually saves - would give us the ability to catch exceptions before making changes typically