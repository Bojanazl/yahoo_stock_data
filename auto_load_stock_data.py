import streamlit as st
import yfinance as yf
import pandas as pd
import psycopg2
import time
from datetime import datetime

table_name = 'stock'
schema_name = 'student'


while True:
    def get_stock_data(symbol):
        try:
            # fetching stock data for today
            # today_date = datetime.today().strftime('%Y-%m-%d') 

            stock = yf.Ticker(symbol)
            ticker_info = stock.info
            ticker_symbol = ticker_info['symbol']
            data = stock.history(period='1d')
            data['Symbol'] = ticker_symbol  # add ticker symbol column
            return data
        except Exception as e:
            st.error(f"Error fetching data for {symbol}: {str(e)}")
            return None

    def save_to_postgresql(data, schema_name, table_name):
        if data is None or data.empty:  # check is the data empty or none
            return

        # connect to Postgresql database (enter your data)
        conn = psycopg2.connect(
            host="your_host",
            database="your_database",
            port="your_port",
            user="your_username",
            password="your_password" # insert your password here
        )
        cursor = conn.cursor()

        # create table
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} "
                       "(date DATE, symbol VARCHAR(10), open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume BIGINT)")

        # get the latest date from the database for the ticker symbol
        cursor.execute(
            f"SELECT * FROM {schema_name}.{table_name} WHERE symbol = %s", (data['Symbol'].iloc[0],))

        existing_data = cursor.fetchall()

        existing_df = pd.DataFrame(existing_data, columns=[
            'date', 'symbol', 'open', 'high', 'low', 'close', 'volume'])

        # filter new data to only include rows not present in the existing data
        new_data = data[~data['Symbol'].isin(existing_df['symbol'])]

        if not new_data.empty:
            for _, row in new_data.iterrows():
                # check if there are any existing rows with the same symbol
                # only compare rows for the symbol that matches
                symbol_matches = existing_df['symbol'] == row['Symbol']

                if symbol_matches.any():
                    existing_rows = existing_df[symbol_matches]

                    # check if any field in the existing rows differs from the new row
                    # negate via ~
                    different_row = existing_rows[~existing_rows.eq(row)].any(axis=1)

                    if different_row.any():
                        # insert the row if there's at least one field that is different
                        cursor.execute(f"INSERT INTO {schema_name}.{table_name} (date, symbol, open, high, low, close, volume) "
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                       (row.name.strftime('%Y-%m-%d'), row['Symbol'], row['Open'], row['High'], row['Low'], row['Close'], row['Volume']))

                        conn.commit()
                        st.success(
                            'New data inserted into the SQL table successfully!')
                else:
                    # insert the row if there are no existing rows with the same symbol
                    cursor.execute(f"INSERT INTO {schema_name}.{table_name} (date, symbol, open, high, low, close, volume) "
                                   "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                                   (row.name.strftime('%Y-%m-%d'), row['Symbol'], row['Open'], row['High'], row['Low'], row['Close'], row['Volume']))

                    conn.commit()
                    st.success(
                        'New data inserted into the SQL table successfully!')

        else:
            st.info('No new data to insert.')

        cursor.close()
        conn.close()

    def main():
        st.title('Stock Data Viewer')

        # list of companies to fetch data for
        company_names = {
            'FDX': 'Fedex',
            '^GSPC': 'Standard & Poor''s 500',
            '^DJI': 'Dow Jones Industrial',
            'MSFT': 'Microsoft',
            'COMP': 'Compass, Inc. ',
            'AAPL': 'Apple',
            'AVGO': 'Avago Technologies Limited (Broadcom)',
            'NBIX': 'Neurocrine Biosciences, Inc.',
            'NVDA': 'Nvidia',
            'NKE': 'Nike',
            'LULU': 'Lululemon',
            'CYBR': 'Cyber Arc Software Ltd.',
            'BLK': 'Black Rock, Inc.',
            'TSLA': 'Tesla',
            'GOOGL': 'Google',
            'AMZN': 'Amazon'
        }

        # List of companies to choose from
        companies = ['FDX', '^GSPC', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', '^DJI', 'COMP', 'AAPL', 'AVGO', 'NBIX',
                     'NVDA', 'NKE', 'LULU', 'CYBR', 'BLK']

        for symbol in companies:
            st.subheader(f'Stock Data for {company_names.get(symbol, symbol)} ({symbol})')
            stock_data = get_stock_data(symbol)

            if stock_data is not None:
                st.write(stock_data)
                save_to_postgresql(stock_data, 'student', 'stock')

    if __name__ == "__main__":
        main()

    time.sleep(10)
    st.rerun()
