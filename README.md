# Week 9 Mini Project 9 
[![CI](https://github.com/nogibjj/IDS706_miniproject2_Mutian/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/IDS706_miniproject2_Mutian/actions/workflows/cicd.yml)
# Goal
 Set up a cloud-hosted notebook and demonstrate data manipulation with a sample dataset

## Overview
I downloaded a csv file from kaggle dataset which is about bitcoin transactions. This project demonstrates some basic statistical info and generates a visualization for data distribution with python libraries like polors,numpy and matplotlib.

Donwload dataset from: https://www.kaggle.com/datasets/jesusgraterol/bitcoin-taker-buysell-volume-binance-futures



## Requirements
* Python (Version 3.6 or newer)
* Polors (Version 0.10.26)
* Matplotlib (Version 3.4.3)
* Datetime(Version 5.2)

## Output
* General info:
  <img width="1351" alt="image" src="https://github.com/nogibjj/IDS706_miniproject9_Mutian/assets/108935314/04e1c4a4-1bb0-4cec-8465-641badad9794">

* Descriptive Statiscs:
  
![img](statiscs.png)


* Buy Vol Plot
  ![img](buy_vol_plot.png)

* Sell Vol Plot
  ![img](sell_vol_plot.png)
  
* Buy Sell Ratio Plot
  ![img](buy_sell_ratio_plot.png)

## Colab Link

https://colab.research.google.com/github/nogibjj/IDS706_miniproject9_Mutian/blob/main/analysis.ipynb

## Run


 1. To run locally, choose a directory and `git clone` this repo. Then use makefile commands like `make run`
 2. Or you can click on the colab link to run remotely.
