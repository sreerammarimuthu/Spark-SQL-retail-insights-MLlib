# Spark SQL Retail Insights + MLlib Regression

This project explores the use of **Spark SQL** and **Spark MLlib** for end-to-end big data processing and regression modeling. We simulate a large-scale retail environment using synthetic customer and transaction datasets, then run SQL transformations to extract insights and apply four regression models to predict purchase totals.  

**Note:** This was an experiment to assess how well regression models perform in a big data setting when limited to **only three input features**.   

## Dataset Description  

Two large-scale synthetic datasets were generated for this project:   

- **Customers Dataset** (`subset_Customers.csv`):  
  - `ID`: Unique integer from 1 to 50,000  
  - `Name`: Random character string (10-20 chars, no commas)  
  - `Age`: Integer between 18 and 100  
  - `CountryCode`: Integer between 1 and 500  
  - `Salary`: Float between 100 and 10,000,000  

- **Purchases Dataset** (`subset_Purchases.csv`):  
  - `TransID`: Unique integer from 1 to 5,000,000  
  - `CustID`: Randomly references a customer ID (approx. 100 purchases per customer)  
  - `TransTotal`: Float between 10 and 2000  
  - `TransNumItems`: Integer between 1 and 15  
  - `TransDesc`: Random text string (20-50 chars, no commas)  

## Contents  

`data/` - Subsets of synthetic datasets used for SQL queries and regression modeling  
- `subset_Customers.csv`  
- `subset_Purchases.csv`  
- `subset_Combined_Dataset.csv` - Join of the above used for ML tasks  

`output/Spark-SQL/` - CSVs from SQL queries  
- `subset_T1_Output.csv` - Purchases with TransTotal ≤ $600  
- `T2_Output.csv` - Purchase stats grouped by TransNumItems  
- `T3_Output.csv` - Aggregated purchases for ages 18-25  
- `subset_T4_Output.csv` - Filtered customer pairs by multi-condition  

`utils+model/`  
- `retail-analytics.ipynb` - Notebook containing full pipeline, from data generation to SQL queries and ML models.  

## Spark SQL Tasks  

| **Task** | **Title**                                   | **Description**                                                                |
| -------- | ------------------------------------------- | ------------------------------------------------------------------------------ |
| T1       | Filter Purchases ≤ \$600                    | Filtered transactions with total ≤ \$600                                       |
| T2       | Purchase Stats by Number of Items           | Min, max, median of TransTotal grouped by TransNumItems                        |
| T3       | Purchases by Young Customers (18-25)        | Aggregated total purchase count and amount for younger customers               |
| T4       | Matching Customer Pairs Based on Conditions | Returned customer pairs satisfying joint conditions on age, amount & items     |

All queries were executed using SQL strings on Spark TempViews, and outputs are saved to `/output/Spark-SQL/`.  

## Regression Model Performance

| **Model**               | **RMSE** | **MAE** | **R²**     |
| ----------------------- | -------- | ------- | ---------- |
| Linear Regression       | 574.37   | 497.48  | -0.0000028 |
| Decision Tree Regressor | 574.38   | 497.49  | -0.0000383 |
| Random Forest Regressor | 574.37   | 497.48  | -0.0000048 |
| GBT Regressor           | 574.39   | 497.49  | -0.0000656 |  

## Conclusion

This regression experiment, deliberately limited to **three features** in a **big data environment** demonstrates the performance bounds when minimal predictive signal is available. Despite dataset scale and model diversity, the resulting metrics (high RMSE/MAE, near-zero or negative R²) confirm that models were unable to uncover meaningful patterns from the chosen features.  
