# Week 10 Mini Project 10 
[![CI](https://github.com/nogibjj/IDS706_miniproject2_Mutian/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/IDS706_miniproject2_Mutian/actions/workflows/cicd.yml)
# Goal
Use PySpark to perform data processing on a large dataset.

## Overview
I downloaded a csv file from kaggle dataset which is about bitcoin transactions. This project demonstrates how to perform data processing, transformation and SQL query with pyspark.
Donwload dataset from: https://www.kaggle.com/datasets/jesusgraterol/bitcoin-taker-buysell-volume-binance-futures



## Output
all the ouput can be found in output.md
* DataFrame:
<img width="460" alt="image" src="https://github.com/nogibjj/IDS706_miniproject10_Mutian/assets/108935314/239cd77b-b026-4d35-a6b4-0a53cdea8e57">


* Data Transformation:
  I defined a new colomn called "buyoversell" which is to do a map function on the column buy_sell_ratio (1 if buy_sell_ration > 1 else 0), then groupby this column and calculate the average value of buy_sell_ratio
  
   <img width="306" alt="image" src="https://github.com/nogibjj/IDS706_miniproject10_Mutian/assets/108935314/663c27c0-3651-476c-97c6-3006e22ef4ea">
* Query data where buy_sell_ratio >2
  
  <img width="413" alt="image" src="https://github.com/nogibjj/IDS706_miniproject10_Mutian/assets/108935314/2ed0b1a1-23e1-489e-a216-f7b79ed05d79">



## Run


 1. To run locally, choose a directory and `git clone` this repo. Then use makefile commands like `make run`
 2. Or you can click on the colab link to run remotely.
