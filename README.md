# Spark SQL Retail Insights with MLlib

This project combines Spark SQL for analytics and Spark MLlib for regression modeling over large-scale retail data. Using generated customer and transaction datasets, we run SQL-based transformations and extract insights before applying and benchmarking four regression models to predict purchase total amounts.

## Contents  

`data/`: Subsets of synthetic retail datasets for SQL queries and machine learning tasks.  
- `subset_Customers.csv` – Randomly generated customer dataset with fields: ID, Name, Age, CountryCode, and Salary.  
- `subset_Purchases.csv` – Synthetic purchase transactions with: TransID, CustID, TransTotal, TransNumItems, and TransDesc.
- `subset_Combined_Dataset.csv` – Joined dataset of purchases and customers, used for regression modeling.

`output/Spark-SQL/`: CSV outputs for each Spark SQL task, exported after query execution.  
- `subset_T1_Output.csv` – All purchases with TransTotal ≤ $600. (Subset of actual output: Top 50)  
- `T2_Output.csv` – Summary stats (min, max, median) grouped by TransNumItems.
- `T3_Output.csv` – Aggregated stats (item count + amount) for young customers (ages 18–25).
- `subset_T4_Output.csv` – Customer pairs satisfying conditions on age, total purchase, and item count. (Subset of actual output: Top 50)  

`utils+model/`: `retail-analytics.ipynb` - Notebook containing full pipeline, from data generation to SQL queries and ML models.  

## Spark SQL Tasks  

| **Task** | **Title**                                   | **Description**                                                                |
| -------- | ------------------------------------------- | ------------------------------------------------------------------------------ |
| T1       | Filter Purchases ≤ \$600                    | Filtered all transactions where purchase total is less than or equal to \$600. |
| T2       | Purchase Stats by Number of Items           | Computed min, max, and median total purchase by `TransNumItems`.               |
| T3       | Purchases by Young Customers (18–25)        | Aggregated total purchase count and amount per customer for ages 18 to 25.     |
| T4       | Matching Customer Pairs Based on Conditions | Returned customer pairs satisfying combined conditions on age, amount & items. |

All queries executed via SQL strings on Spark TempViews, and then these outputs were saved in `/output/Spark-SQL/`.   

## Regression Model Performance

| **Model**               | **RMSE** | **MAE** | **R²**     |
| ----------------------- | -------- | ------- | ---------- |
| Linear Regression       | 574.37   | 497.48  | -0.0000028 |
| Decision Tree Regressor | 574.38   | 497.49  | -0.0000383 |
| Random Forest Regressor | 574.37   | 497.48  | -0.0000048 |
| GBT Regressor           | 574.39   | 497.49  | -0.0000656 |

All models yielded similar performance, this happened likely due to minimal feature signal in our synthetic data.
