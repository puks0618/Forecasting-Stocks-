# Stock Price Forecasting using yFinance & Airflow  

## About the Project  
This project automates stock price tracking and forecasting. Using the yFinance API, we pull real-time stock data, process it, and apply machine learning (ML) models to predict future stock prices. The entire workflow is orchestrated and scheduled using Apache Airflow, ensuring a smooth and reliable data pipeline.  

## Why This Project?  
Stock market data changes every second, making manual tracking impractical. This project automates the entire process, enabling efficient trend analysis, forecasting, and data-driven decision-making.  

## How It Works  
- ETL Pipeline (Extract, Transform, Load): Fetches stock data from yFinance, cleans it, and stores it in a database.  
- ML Forecasting Pipeline: Uses historical data to predict future stock prices.  
- Airflow DAGs: Orchestrates and schedules the entire pipeline at set intervals.  
- Final Table: Merges actual stock data with predictions for a complete market overview.  

## Tech Stack  
- Python  
- yFinance API  
- Apache Airflow  
- Snowflake  
- Machine Learning (Time-Series Forecasting)   

## Database Design  

### Stock Data Table (stock_data)  
Stores real-time stock prices from yFinance.  

| Column Name  | Type     | Description                  |  
|-------------|----------|------------------------------|  
| id          | INT      | Unique record ID             |  
| symbol      | VARCHAR  | Stock ticker (e.g., AAPL, TSLA) |  
| date        | TIMESTAMP | Date and time of data        |  
| open_price  | FLOAT    | Opening price                |  
| close_price | FLOAT    | Closing price                |  
| high        | FLOAT    | Highest price of the day     |  
| low         | FLOAT    | Lowest price of the day      |  
| volume      | INT      | Trading volume               |  

### Forecast Data Table (forecast_data)  
Stores predicted stock prices.  

| Column Name      | Type     | Description               |  
|------------------|----------|---------------------------|  
| id              | INT      | Unique record ID          |  
| symbol          | VARCHAR  | Stock ticker              |  
| prediction_date | TIMESTAMP | Future date for prediction |  
| predicted_price | FLOAT    | Forecasted stock price    |  

### Final Analysis Table (final_stock_analysis)  
Merges actual and predicted stock prices for a complete view.  

| Column Name  | Type     | Description                     |  
|-------------|----------|---------------------------------|  
| id          | INT      | Unique record ID               |  
| symbol      | VARCHAR  | Stock ticker                   |  
| date        | TIMESTAMP | Actual or predicted date       |  
| price       | FLOAT    | Stock price (actual or predicted) |  
| source      | VARCHAR  | actual or predicted            |  

## How Airflow Handles It  

### yFinance ETL Pipeline DAG  
- Fetches stock data using yFinance.  
- Cleans and processes the data.  
- Stores it in the stock_data table.  

### ML Forecasting Pipeline DAG  
- Uses historical data to train an ML model.  
- Predicts future stock prices.  
- Stores forecasts in the forecast_data table.  
- Merges real and predicted data into final_stock_analysis.  

## Cool Features  
- Uses Airflow Connections & Variables for secure database credentials.  
- SQL Transactions with Error Handling to maintain data integrity.  
- Automated Data Merging for actual and forecasted stock prices.  
- Scalable & Modular – new stocks can be added easily.  
- Scheduled Execution – no manual work required.  
