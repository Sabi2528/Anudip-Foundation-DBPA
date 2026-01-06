import yfinance as yf
import pandas as pd
import mysql.connector
from datetime import datetime
import time 

print("Starting Real-time Reliance Predictor...")

while True: 
    try:
        ticker = "RELIANCE.NS"
        # 1-minute interval data (Intraday)
        data = yf.download(ticker, period='7d', interval='1m')

        # Technical Indicators Calculation
        delta = data['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        data['RSI'] = 100 - (100 / (1 + rs))
        data['MA20'] = data['Close'].rolling(window=20).mean()
        data['MA50'] = data['Close'].rolling(window=50).mean()
        data['Price_Change_Pct'] = data['Close'].pct_change() * 100

        mydb = mysql.connector.connect(
          host="localhost",
          user="root",        
          password="Abitha28", 
          database="StockDB"  
        )
        cursor = mydb.cursor()

        cursor.execute("TRUNCATE TABLE stock_data_enhanced")

        count = 0
        for i in range(len(data)):
            current_rsi = data['RSI'].iloc[i]
            if not pd.isna(current_rsi):
                signal = "HOLD"
                if current_rsi < 30: signal = "BUY"
                elif current_rsi > 70: signal = "SELL"

                val = (
                    data.index[i].to_pydatetime(),
                    ticker,
                    float(data['Close'].iloc[i]),
                    float(data['High'].iloc[i]),
                    float(data['Low'].iloc[i]),
                    int(data['Volume'].iloc[i]),
                    float(current_rsi),
                    float(data['MA20'].iloc[i]) if not pd.isna(data['MA20'].iloc[i]) else None,
                    float(data['MA50'].iloc[i]) if not pd.isna(data['MA50'].iloc[i]) else None,
                    float(data['Price_Change_Pct'].iloc[i]) if not pd.isna(data['Price_Change_Pct'].iloc[i]) else 0,
                    signal
                )
                sql = """INSERT INTO stock_data_enhanced 
                         (Date, Symbol, Price, High, Low, Volume, RSI, MA20, MA50, Pct_Change, Signal_Type) 
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cursor.execute(sql, val)
                count += 1
        
        mydb.commit()
        print(f"Update Successful! {count} rows saved at {datetime.now().strftime('%H:%M:%S')}")
        
        cursor.close()
        mydb.close()
        
        
        time.sleep(60) 

    except Exception as err:
        print(f"Error: {err}")
        time.sleep(10) 