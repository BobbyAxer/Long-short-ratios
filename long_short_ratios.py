import requests
import json
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
load_dotenv()
import os
import time
import mplfinance as mpf
import asyncio
import aiohttp

api_key_binance = os.environ.get('API_B')
api_secret_binance = os.environ.get('SECRET_B')

async def get_binance_futures_tickers():
    url = 'https://fapi.binance.com/fapi/v1/ticker/24hr'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
    futures_tickers = [ticker['symbol'] for ticker in data if 'USDT' in ticker['symbol']]
    return futures_tickers

async def get_data(symbol, period, limit):
    endpoint = 'https://fapi.binance.com/futures/data/globalLongShortAccountRatio'
    headers = {
        'X-MBX-APIKEY': api_key_binance
    }
    params = {
        'symbol': symbol,
        'period': period,
        'limit': limit
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(endpoint, headers=headers, params=params) as response:
            data = await response.json()
    return data

async def main():
    start = time.time()
    tickers = await get_binance_futures_tickers()
    print(tickers)
    data = []

    tasks = []
    for symbol in tickers:
        task = asyncio.ensure_future(get_data(symbol, '1h', 300))
        tasks.append(task)

    responses = await asyncio.gather(*tasks)
    for symbol_data in responses:
        print(symbol_data)
        for row in symbol_data:
            # row["symbol"] = symbol
            data.append(row)


    df = pd.DataFrame(data)
    # print(df)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms')
    df = df.set_index("timestamp")
    df["longShortRatio"] = df["longShortRatio"].astype(float)
    print(df)
    # avg_ls_rate = round(df.groupby("symbol")["longShortRatio"].mean(), 2)
    # four_hours = round(df.groupby("symbol").apply(lambda x: x.tail(4)["longShortRatio"].mean()), 2)
    # # # Get the last funding rate for each symbol
    # last_ls = round(df.groupby("symbol")["longShortRatio"].last(), 2)
    # last_day = round(df.groupby("symbol").apply(lambda x: x.tail(24)["longShortRatio"].mean()), 2)
    # # # print(seven_days)
    # ten_days = round(df.groupby("symbol").apply(lambda x: x.tail(240)["longShortRatio"].mean()), 2)
    df["1h_pct_change"] = df.groupby("symbol")["longShortRatio"].pct_change(periods=1)
    df["4h_pct_change"] = df.groupby("symbol")["longShortRatio"].pct_change(periods=4)
    df["24h_pct_change"] = df.groupby("symbol")["longShortRatio"].pct_change(periods=24)
    df["7d_pct_change"] = df.groupby("symbol")["longShortRatio"].pct_change(periods=168)
    df["all_time_pct_change"] = df.groupby("symbol")["longShortRatio"].pct_change()
    # iloc[-25]).round(2)
    avg_ls_rate = round(df.groupby("symbol")["longShortRatio"].mean(), 2)
    four_hours = round(df.groupby("symbol")["longShortRatio"].apply(lambda x: x.iloc[-5]), 2)
    # # Get the last funding rate for each symbol
    last_ls = round(df.groupby("symbol")["longShortRatio"].last(), 2)
    last_day = df.groupby("symbol")["longShortRatio"].apply(lambda x: x.shift(24).iloc[-1]).round(2)
    # # print(seven_days)
    ten_days = df.groupby("symbol")["longShortRatio"].apply(lambda x: x.shift(10*24).iloc[-1]).round(2)

    # # Concatenate the two results into a single dataframe
    # one_day = round(df.groupby("symbol").apply(lambda x: x.tail(3)["fundingRate"].mean() * 365 * 3 * 100), 2)
    result = pd.concat([avg_ls_rate, last_ls, four_hours, last_day, ten_days,
                        df.groupby("symbol")["1h_pct_change"].last(),
                        df.groupby("symbol")["4h_pct_change"].last(),
                        df.groupby("symbol")["24h_pct_change"].last(),
                        df.groupby("symbol")["7d_pct_change"].last(),
                        df.groupby("symbol")["all_time_pct_change"].last()], axis=1)
    result.columns = ["avg LS", 'last LS', '4h', '1day', '10 day',                   '1h_pct_change', '4h_pct_change', '24h_pct_change', '7d_pct_change', 'all_time_pct_change']

    # # Print the result
    print("Top 15 symbols with highest LAST ls:")
    print(result.nlargest(15, "1h_pct_change")[["last LS", '1h_pct_change',"24h_pct_change", "all_time_pct_change"]])
    print("Top 15 symbols with lowest LAST ls:")
    print(result.nsmallest(15, "1h_pct_change")[["last LS", '1h_pct_change',"24h_pct_change", "all_time_pct_change"]])
    print("Top 15 symbols with highest 1day LS:")
    print(result.nlargest(15, "4h")[["last LS", '1day',"4h_pct_change", "all_time_pct_change"]])
    print("Top 15 symbols with lowest last 1day LS:")
    print(result.nsmallest(15, "4h")[["last LS", '1day',"4h_pct_change", "all_time_pct_change"]])


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
