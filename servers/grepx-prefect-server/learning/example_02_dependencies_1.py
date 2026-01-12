"""
Prefect Learning Example 02: Dependencies Part 2
=================================================
More complex example with multiple dependencies and branching
"""
from prefect import flow, task
from typing import List, Dict
import random


@task(name="get_stock_data")
def get_stock_data(symbols: List[str]) -> Dict[str, float]:
    """Simulate getting stock prices for given symbols"""
    print(f"Getting stock data for symbols: {symbols}")
    stock_data = {}
    for symbol in symbols:
        # Generate random prices between $10 and $200
        price = round(random.uniform(10, 200), 2)
        stock_data[symbol] = price
    
    print(f"Retrieved prices: {stock_data}")
    return stock_data


@task(name="calculate_moving_average")
def calculate_moving_average(prices: Dict[str, float], window: int = 5) -> Dict[str, float]:
    """Calculate moving average (simulated with simple logic)"""
    print(f"Calculating {window}-day moving average for prices: {prices}")
    # For simplicity, we'll return a modified version of the prices
    moving_averages = {}
    for symbol, price in prices.items():
        # Simulate moving average as a value close to the current price
        avg = round(price * (0.95 + random.uniform(0, 0.1)), 2)
        moving_averages[symbol] = avg
    
    print(f"Moving averages: {moving_averages}")
    return moving_averages


@task(name="calculate_volatility")
def calculate_volatility(prices: Dict[str, float]) -> Dict[str, float]:
    """Calculate volatility (simulated)"""
    print(f"Calculating volatility for prices: {prices}")
    volatility = {}
    for symbol, price in prices.items():
        # Calculate volatility as a percentage of the price
        vol = round(random.uniform(0.5, 5.0), 2)  # 0.5% to 5% volatility
        volatility[symbol] = vol
    
    print(f"Volatility values: {volatility}")
    return volatility


@task(name="generate_trading_signal")
def generate_trading_signal(
    current_prices: Dict[str, float], 
    moving_averages: Dict[str, float], 
    volatility: Dict[str, float]
) -> Dict[str, str]:
    """Generate trading signals based on price vs moving average and volatility"""
    print("Generating trading signals...")
    signals = {}
    
    for symbol in current_prices.keys():
        if symbol in moving_averages and symbol in volatility:
            current = current_prices[symbol]
            ma = moving_averages[symbol]
            vol = volatility[symbol]
            
            # Simple signal logic: if price > moving average, consider it bullish
            if current > ma:
                if vol > 3.0:  # High volatility
                    signals[symbol] = "STRONG_BUY"
                else:
                    signals[symbol] = "BUY"
            elif current < ma:
                if vol > 3.0:  # High volatility
                    signals[symbol] = "STRONG_SELL"
                else:
                    signals[symbol] = "SELL"
            else:
                signals[symbol] = "HOLD"
    
    print(f"Trading signals: {signals}")
    return signals


@task(name="execute_trades")
def execute_trades(signals: Dict[str, str], cash_balance: float = 10000.0) -> Dict[str, float]:
    """Execute trades based on signals (simulated)"""
    print(f"Executing trades with cash balance: ${cash_balance}")
    print(f"Signals to execute: {signals}")
    
    # Simulate portfolio changes based on signals
    portfolio_value = cash_balance
    for symbol, signal in signals.items():
        if signal in ["BUY", "STRONG_BUY"]:
            # Simulate buying and potential profit/loss
            change = random.uniform(0.95, 1.05)  # -5% to +5% change
            portfolio_value *= change
        elif signal in ["SELL", "STRONG_SELL"]:
            # Simulate selling and potential profit/loss
            change = random.uniform(0.98, 1.02)  # -2% to +2% change
            portfolio_value *= change
    
    result = {
        "initial_balance": cash_balance,
        "final_balance": round(portfolio_value, 2),
        "change": round(portfolio_value - cash_balance, 2),
        "change_percent": round(((portfolio_value - cash_balance) / cash_balance) * 100, 2)
    }
    
    print(f"Trade execution result: {result}")
    return result


@flow(name="Trading Algorithm Flow with Multiple Dependencies")
def trading_algorithm_flow(
    stock_symbols: List[str] = ["AAPL", "GOOGL", "MSFT"],
    cash_balance: float = 10000.0
):
    """
    A flow that demonstrates:
    - Multiple dependencies between tasks
    - Branching where multiple tasks depend on one task
    - Complex data flow between tasks
    """
    print("Starting trading algorithm flow...")
    
    # Get stock data (base for all other calculations)
    stock_prices = get_stock_data(stock_symbols)
    
    # Calculate moving averages based on prices
    moving_avgs = calculate_moving_average(stock_prices)
    
    # Calculate volatility based on prices
    volatility_values = calculate_volatility(stock_prices)
    
    # Generate trading signals based on prices, moving averages, and volatility
    signals = generate_trading_signal(stock_prices, moving_avgs, volatility_values)
    
    # Execute trades based on the signals
    trade_results = execute_trades(signals, cash_balance)
    
    print(f"Flow completed with results: {trade_results}")
    return trade_results


if __name__ == "__main__":
    # Run the flow
    result = trading_algorithm_flow(["AAPL", "TSLA", "MSFT", "AMZN"], 5000.0)
    print(f"Final result: {result}")