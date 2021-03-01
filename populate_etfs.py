from dotenv import load_dotenv
load_dotenv()
import csv
import os
import psycopg2
import psycopg2.extras

connection = psycopg2.connect(host=os.getenv("DB_HOST"), database=os.getenv("DB_NAME"), user=os.getenv("DB_USER"), password=os.getenv("DB_PASS"))

cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

cursor.execute("SELECT * FROM stock WHERE is_etf = True")

etfs = cursor.fetchall()

dates = ['2021-02-26']

for current_date in dates: # at each date
    for etf in etfs: # loop through etf holdings at that date
        print(etf['symbol'])
        with open(f"data/{current_date}/{etf['symbol']}.csv") as f:
            reader = csv.reader(f)
            next(reader) # skip header
            for row in reader: 
                ticker = row[3]
                
                if ticker:
                    shares = row[5] 
                    weight = row[7]
                    cursor.execute("""
                    SELECT * FROM stock WHERE symbol = %s
                    """, (ticker,))
                    stock = cursor.fetchone()
                    if stock:
                        cursor.execute("""
                            INSERT INTO etf_holding (etf_id, holding_id, dt, shares, weight)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (etf['id'], stock['id'], current_date, shares, weight))
                        
connection.commit()
                    
