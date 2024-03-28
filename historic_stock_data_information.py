import streamlit as st
import yfinance as yf
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import psycopg2
from pymongo import MongoClient
from datetime import datetime

st.set_page_config(layout='wide')  # wide view


def local_css(style):
    with open(style) as f:
        st.markdown('<style>{}</style>'.format(f.read()),
                    unsafe_allow_html=True)


local_css("style_yahoo.css")

# we have two connections: to POSTGRESQL (local) and MongoDB (cloud)

# 1. POSTGRESQL
table_name = 'historic_stock'
schema_name = 'student'


# connection parameters (enter yours)
conn_params = {
    "host": "your_host",
    "database": "your_database",
    "port": "your_port",
    "user": "your_username",
    "password": "password" #insert your password
}

conn = psycopg2.connect(**conn_params)
# print("connection established") #to test connection


# inert data into POSTGRESQL table names historic_stock
def insert_data_to_postgres(conn, data, table_name, schema_name):
    cursor = conn.cursor()
    print("trying to create the table")

    # create table
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} "
                   "(date DATE, symbol VARCHAR(10), open FLOAT, high FLOAT, low FLOAT, close FLOAT, adjusted_close FLOAT, volume FLOAT)")
    print("table created")

    # query existing data for the ticker symbol
    cursor.execute(
        f"SELECT * FROM {schema_name}.{table_name} WHERE symbol = %s", (data['Symbol'].iloc[0],))

    existing_data = cursor.fetchall()

    # convert data to DataFrame for easier comparison
    existing_df = pd.DataFrame(existing_data, columns=[
        'date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'adjusted_close'])

    # filter new data to only include rows not present in the existing data
    new_data = data[~data.index.isin(existing_df['date'])]

    if not new_data.empty:
        for _, row in new_data.iterrows():
            cursor.execute(f"INSERT INTO {schema_name}.{table_name} (date, symbol, open, high, low, close, volume) "
                           "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                           (row.name.strftime('%Y-%m-%d'), row['Symbol'], row['Open'], row['High'], row['Low'], row['Close'], row['Volume']))

        conn.commit()
        st.success('New data inserted into the SQL table successfully!')
    else:
        st.info('No new data to insert.')

    cursor.close()


# 2. connect to MongoDB (enter your information)
# paste your password
mongo_client = MongoClient(
    "your_client_address")
db = mongo_client['your_db_name']
collection = db['your_collection_name']  # collection in MongoDB


# define containers
header = st.container(height=None, border=True)
choice = st.container(height=None, border=True)
history = st.container(height=None, border=True)
compare = st.container(height=None, border=True)
plot = st.container(height=None, border=True)


# we have two sections: 1) SELECTION to choose a company and 2) COMPARISON to compare to other company

# -------Selection section---------
with header:
    st.title('Stock Data Information')

with choice:
    col1, col2 = st.columns(2)

    with col1:
        # ticker seleciton
        ticker_symbols = {'FDX': 'Fedex',
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
                          'TSLA': 'Tesla'}

        # dropdown menu to select the Company
        selected_ticker = st.selectbox('Select the Company:',
                                       [(f"{ticker} ({ticker_symbols[ticker]})") for ticker in ticker_symbols.keys()], key="ticker_1")

        ticker_symbol_1 = selected_ticker.split()[0]

        # date selection
        start_date = st.date_input(
            "Start Date", value=datetime(2024, 1, 1), key="start_date_1")
        end_date = st.date_input("End Date", value=datetime(
            2024, 3, 22), key="end_date_1")

        # -----TABLE 1-----
        # extract the data to be shown
        stock_data = yf.Ticker(ticker_symbol_1)
        day_high = stock_data.info.get('dayHigh', None)
        day_low = stock_data.info.get('dayLow', None)
        last_price = stock_data.info.get('currentPrice', None)
        last_volume = stock_data.info.get('regularMarketVolume', None)
        open_price = stock_data.info.get('open', None)

        # create the dictionary
        stock_data_dict = {
            'Attribute': ['Symbol', 'Day High', 'Day Low', 'Last Price', 'Last Volume', 'Open'],
            'Value': [ticker_symbol_1, day_high, day_low, last_price, last_volume, open_price]
        }

        # convert dictinary to data frame
        stock_df = pd.DataFrame(stock_data_dict)

        # transpose the data frame so attributes are columns
        stock_df = stock_df.transpose()

        # set first row to be a column header
        stock_df.columns = stock_df.iloc[0]
        stock_df = stock_df[1:]

        # display the table
        st.write(stock_df)

    with col2:
        company_info = yf.Ticker(ticker_symbol_1).info

        # company information
        st.markdown("<p style='text-align:right'><b>Company Info:</b>",
                    unsafe_allow_html=True)
        st.markdown(f"<p style='text-align:right'><b>Address:</b> {company_info.get('address1', '')} | <b>City:</b> {
            company_info.get('city', '')}", unsafe_allow_html=True)
        st.markdown(f"<p style='text-align:right'><b>Zip:</b> {company_info.get('zip', '')} | <b>Country: </b>{
            company_info.get('country', '')}", unsafe_allow_html=True)
        st.markdown(f"<p style='text-align:right'><b>Phone:</b> {company_info.get('phone', '')} | | <b>Website: </b>{
            company_info.get('website', '')}", unsafe_allow_html=True)
        st.markdown(
            f"<p style='text-align:right'><b>Industry</b>: {company_info.get('industry', '')}", unsafe_allow_html=True)

st.markdown('<span id="custom-container"></span>', unsafe_allow_html=True)
with history:
    col1, col2 = st.columns(2)
    with col1:
        # -----TABLE 2-----
        # fetch data for selected dates and ticker
        data_1 = yf.download(ticker_symbol_1, start=start_date, end=end_date)
        data_1['Symbol'] = ticker_symbol_1 # symbol column for the ticker

        # display stock history data
        st.subheader('Stock History Data')
        st.write(data_1)

        # create button to save the historic data to MongoDB
        st.markdown('<span id="button-after"></span>', unsafe_allow_html=True) # style

        if st.button('Save to MongoDB'):
            data_dict = data_1.reset_index().to_dict(orient='records')
            collection.insert_many(data_dict)
            st.success('Data saved to MongoDB successfully!')

        # create button to export data to POSTGRESQL database
        st.markdown('<span id="button-after"></span>',unsafe_allow_html=True)  # style

        if st.button('Export data to SQL database'):
            insert_data_to_postgres(conn, data_1, 'historic_stock', 'student')
            conn.close()

        # create the button to export the historic data to .csv
        st.markdown('<span id="button-after"></span>',
                    unsafe_allow_html=True)  # style
        
        if st.button('Export data to CSV'):
            mongo_data = list(collection.find({}))
            df_mongo = pd.DataFrame(mongo_data)
            df_mongo.to_csv('mongo_data.csv', index=False)
            st.success('Data exported to CSV successfully!')

    with col2:
        st.markdown('<span id="column"></span>', unsafe_allow_html=True) # style

        # plot for the first company only
        fig_company, ax1 = plt.subplots(figsize=(15, 15))
        ax1.plot(data_1['Open'], label='Open')
        ax1.plot(data_1['High'], label='High')
        ax1.plot(data_1['Low'], label='Low')
        ax1.plot(data_1['Close'], label='Close')

        ax1.set_title(f"Stock Data for {ticker_symbol_1}")
        ax1.set_xlabel("Date")
        ax1.set_ylabel("Value")
        ax1.legend()

        plt.tight_layout()
        st.pyplot(fig_company)


# -------Comparison section---------

with compare:
    st.subheader("Compare with another company:")
    # select another company
    ticker_symbols_2 = {'FDX': 'Fedex',
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
                        'TSLA': 'Tesla'}
    selected_ticker = st.selectbox('Select the Company:',
                                   [(f"{ticker} ({ticker_symbols_2[ticker]})") for ticker in ticker_symbols_2.keys()], key="ticker_2")

    ticker_symbol_2 = selected_ticker.split()[0]

    start_date_2 = st.date_input(
        "Start Date", value=datetime(2024, 1, 1), key="start_date_2")
    end_date_2 = st.date_input("End Date", value=datetime(
        2024, 3, 22), key="end_date_2")

    # fetch data for the second ticker
    data_2 = yf.download(ticker_symbol_2, start=start_date_2, end=end_date_2)

    # display data
    st.write(f"Stock data for {ticker_symbol_2}:")
    st.write(data_2)

with plot:
    # select attributes
    st.subheader("Select Data Attributes to Plot:")
    plot_open = st.checkbox("Open")
    plot_high = st.checkbox("High")
    plot_low = st.checkbox("Low")
    plot_close = st.checkbox("Close")
    # volume is in millions - renders other attributes VISUALLY to "zero"
    # the best is to select the volume alone when choosing the attribute
    plot_volume = st.checkbox("Volume")

    # plot the data for each company and both companies (three in total)
    if st.button('Plot Comparison'):
        fig = plt.figure(figsize=(15, 10))
        gs = fig.add_gridspec(2, 2, height_ratios=[2, 1])

        # plot for the first ticker symbol
        ax1 = fig.add_subplot(gs[0])
        if plot_open:
            ax1.plot(data_1['Open'], label='Open')
        if plot_high:
            ax1.plot(data_1['High'], label='High')
        if plot_low:
            ax1.plot(data_1['Low'], label='Low')
        if plot_close:
            ax1.plot(data_1['Close'], label='Close')
        if plot_volume:
            ax1.plot(data_1['Volume'], label='Volume in Mil')

        ax1.set_title(f"Stock Data for {ticker_symbol_1}")
        ax1.set_xlabel("Date")
        ax1.set_ylabel("Value")
        ax1.legend()

        # plot for the second ticker symbol
        ax2 = fig.add_subplot(gs[1])
        if plot_open:
            ax2.plot(data_2['Open'], label='Open')
        if plot_high:
            ax2.plot(data_2['High'], label='High')
        if plot_low:
            ax2.plot(data_2['Low'], label='Low')
        if plot_close:
            ax2.plot(data_2['Close'], label='Close')
        if plot_volume:
            ax2.plot(data_2['Volume'], label='Volume in Mil')

        ax2.set_title(f"Stock Data for {ticker_symbol_2}")
        ax2.set_xlabel("Date")
        ax2.set_ylabel("Value")
        ax2.legend()

        # plot for both ticker symbols
        ax3 = fig.add_subplot(gs[2])
        if plot_open:
            ax3.plot(data_1['Open'], label=f'{ticker_symbol_1} Open')
            ax3.plot(data_2['Open'], label=f'{ticker_symbol_2} Open')
        if plot_high:
            ax3.plot(data_1['High'], label=f'{ticker_symbol_1} High')
            ax3.plot(data_2['High'], label=f'{ticker_symbol_2} High')
        if plot_low:
            ax3.plot(data_1['Low'], label=f'{ticker_symbol_1} Low')
            ax3.plot(data_2['Low'], label=f'{ticker_symbol_2} Low')
        if plot_close:
            ax3.plot(data_1['Close'], label=f'{ticker_symbol_1} Close')
            ax3.plot(data_2['Close'], label=f'{ticker_symbol_2} Close')
        if plot_volume:
            ax3.plot(data_1['Volume'], label=f'{
                     ticker_symbol_1} Volume in Mil')
            ax3.plot(data_2['Volume'], label=f'{
                     ticker_symbol_2} Volume in Mil')

        ax3.set_title(f"Comparison of {ticker_symbol_1} and {ticker_symbol_2}")
        ax3.set_xlabel("Date")
        ax3.set_ylabel("Value")
        ax3.legend()

        plt.tight_layout()
        st.pyplot(fig)
