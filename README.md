# KRX Market Data Conversion

This repository contains Python scripts for converting original KRX (Korea Exchange) market data into a custom CSV format, ready to be pushed into TimescaleDB for storage and further analysis.

## Overview

KRX market data consists of more than 50 unique transaction codes, each with its own format and structure. To efficiently process this native market data, a new data structure was created within these Python scripts. The conversion scripts automate data transformation and ensure that all relevant data points are captured in a unified CSV format, which is then pushed into TimescaleDB.

## Key Features

- **Automated Data Conversion**: Python scripts are scheduled with crontab to automatically convert market data at regular intervals and store it in TimescaleDB.
- **Transaction Code Handling**: Supports processing of over 50 different transaction codes.
- **Custom Data Structure**: Implements a new data structure for efficiently handling KRX native market data.

## Files

The repository contains the following Python scripts for each transaction code:

1. `convert_transaction_code_1.py` - Handles conversion for transaction code 1.
2. `convert_transaction_code_2.py` - Handles conversion for transaction code 2.
3. `convert_transaction_code_3.py` - Handles conversion for transaction code 3.
4. `convert_transaction_code_4.py` - Handles conversion for transaction code 4.
5. `convert_transaction_code_5.py` - Handles conversion for transaction code 5.
6. `convert_transaction_code_6.py` - Handles conversion for transaction code 6.
7. `convert_transaction_code_7.py` - Handles conversion for transaction code 7.

## Automation with Crontab

These scripts are designed to run automatically using `crontab`, ensuring continuous and timely data processing. Market data is automatically converted and stored in the TimescaleDB time-series database at regular intervals.

### Example Crontab Configuration

To schedule the scripts with crontab:

1. Open your crontab file:
   ```bash
   crontab -e
2. Add an entry to run the script at a specific time interval (e.g., every day at midnight):
   ```bash
   0 0 * * * /usr/bin/python3 /path/to/krx_md/convert_transaction_code_X.py >> /path/to/logfile.log 2>&1
