import json 
from decimal import Decimal
import threading
from datetime import datetime
from data_manager import DataManager
from config import initialize_client
from app_logging import logger, info, error
import time
import math
import pandas as pd
from threading import Lock
from purchase_logic import execute_bulk_buy_orders


data_manager = DataManager('purchase_point.json', 'currency.json')

global usdt_balance
usdt_balance = Decimal('0.0')  # Initialize with 0.0
update_Nearest_purchase_point = 0
market_data_handling = 0



def get_wallet_balance(client, asset):
    global usdt_balance  # Declare the use of the global variable
    try:
        account_info = client.get_account()
        balances = account_info['balances']
        for balance in balances:
            if balance['asset'] == asset:
                usdt_balance = Decimal(balance['free'])  # Update the global balance variable
                logger.info(f"Fetched wallet balance for {asset}: {usdt_balance}")  # Log the updated balance
                return usdt_balance
        logger.info(f"Asset {asset} not found in account balances.")  # Log if asset not found
        return Decimal('0.0')  # Return 0 if the asset is not found
    except Exception as e:
        logger.error(f"Error fetching wallet balance for {asset}: {str(e)}")
        return Decimal('0.0')  # Return 0.0 in case of an error



def update_Nearest_purchase_point_func(data_manager):
    global update_Nearest_purchase_point
    if update_Nearest_purchase_point != 0:
        #logger.info("Update process is already running.")
        return
    update_Nearest_purchase_point = 1
    
    try:
        # Step 2: Iterate through market data to get live prices
        for symbol in data_manager.market_data['symbol']:
            live_price = data_manager.market_data[data_manager.market_data['symbol'] == symbol]['price'].iloc[0]
            currency_row = data_manager.currency_data[data_manager.currency_data['symbol'] == symbol]

            # Step 3: Check currency status and price range
            if not currency_row.empty and currency_row['status'].iloc[0] == 0:
                if not (currency_row['display_purchase_point_start'].iloc[0] <= live_price <= currency_row['display_purchase_point_end'].iloc[0]):
                    # If the live price is outside the range, find and update nearest purchase points
                    data_manager.find_and_update_nearest_purchase_points(symbol, live_price)

                    # Assuming a method to update display purchase points here (as described in your requirement)
                    data_manager.update_display_purchase_points_based_on_top_10(symbol)

        #logger.info("All tasks completed successfully.")
    except Exception as e:
        logger.error(f"Error during the update process: {e}")
    finally:
        update_Nearest_purchase_point = 0

def manage_market_data(client, data_manager):
    global market_data_handling, usdt_balance
    if market_data_handling:
        #logger.info("Market data management is already running.")
        return
    market_data_handling = 1

    try:
        if usdt_balance < Decimal('10'):
            usdt_balance = get_wallet_balance(client, 'USDT')
        max_purchases = min(math.floor(usdt_balance / Decimal('10')), 50)
        #logger.info(f"Checking symbols for market updates. Max possible purchases: {max_purchases}")

        eligible_orders = []
        for symbol in data_manager.market_data['symbol'].unique():
            live_price = data_manager.market_data.loc[data_manager.market_data['symbol'] == symbol, 'price'].iloc[0]
            purchase_points = data_manager.get_eligible_purchase_points(symbol)

            for _, point in purchase_points.iterrows():
                start_price = min(point['purchase_point_start'], point['purchase_point_end'])
                end_price = max(point['purchase_point_start'], point['purchase_point_end'])

                if start_price <= live_price <= end_price:
                    eligible_orders.append({'symbol': symbol, 'price': live_price, 'stock_id': point['stock_id']})
                    # Update status to 3 to mark as purchasing, preventing duplicate triggers
                    data_manager.purchase_point.loc[data_manager.purchase_point['stock_id'] == point['stock_id'], 'status'] = 3
                    #logger.info(f"Checking DataFrame in after setting 3: {data_manager.purchase_point.to_string()}")
                    #logger.info(f"Triggering purchase for {symbol} at price {live_price} for stock ID {point['stock_id']} within range [{start_price}, {end_price}]")

        if eligible_orders:
            execute_bulk_buy_orders(client, eligible_orders)
            usdt_balance -= Decimal(len(eligible_orders) * 10)
            #logger.info(f"Executed bulk buy orders. Remaining USDT balance: {usdt_balance}")

    finally:
        market_data_handling = 0
        ##logger.info("Completed managing market data.")
