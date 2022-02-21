# Norn-Finance-API-Server

![Build Badge](https://github.com/zmcx16/Norn-Finance-API-Server/workflows/build/badge.svg)
[![codecov](https://codecov.io/gh/zmcx16/Norn-Finance-API-Server/branch/master/graph/badge.svg?token=5KRR9JSM0C)](https://codecov.io/gh/zmcx16/Norn-Finance-API-Server)

A lightweight finance API server to get the US stocks, options data, and calculate the option valuation.

# Support APIs
* [GET]        /stock/history
* [GET]        /option/quote
* [GET]        /option/quote-valuation
* [Websocket]  /option/quote-valuation

API documents: https://norn-finance.zmcx16.moe/docs

# Volatility Calculator
* Historical Volatility
* Avg Historical Volatility
* EWMA Historical Volatility

# Options Valuation Model
* Black-Scholes-Merton
* Monte Carlo
* Binomial Tree

# DataSource 
* Yahoo Finance
* MarketWatch

# Demo
https://norn-stockscreener.zmcx16.moe/options/

![image](https://github.com/zmcx16/Norn-Finance-API-Server/blob/master/demo/demo1.png)

![image](https://github.com/zmcx16/Norn-Finance-API-Server/blob/master/demo/demo2.png)

# Reference
* QSCTech-Sange / Options-Calculator - (https://github.com/QSCTech-Sange/Options-Calculator)

# License
This project is licensed under the terms of the MIT license.
