## Solution
## A. Data Cleansing Steps
In a single query, perform the following operations and generate a new table in the ```data_mart``` schema named ```clean_weekly_sales```:
  * Convert the ```week_date``` to a ```DATE``` format
  * Add a ```week_number``` as the second column for each ```week_date``` value, for example any value from the 1st of January to 7th of January will be 1, 8th to 14th will be 2 etc
  * Add a ```month_number``` with the calendar month for each ```week_date``` value as the 3rd column
  * Add a ```calendar_year``` column as the 4th column containing either 2018, 2019 or 2020 values
  * Add a new column called ```age_band``` after the original ```segment``` column using the following mapping on the number inside the ```segment``` value

| egment | age_band     |
|--------|--------------|
| 1      | Young Adults |
| 2      | Middle Aged  |
| 3 or 4 | Retirees     |
  
  * Add a new ```demographic``` column using the following mapping for the first letter in the ```segment``` values
  
| segment | demographic |
|---------|-------------|
| C       | Couples     |
| F       | Families    |
  
  * Ensure all ```null``` string values with an ```"unknown"``` string value in the original ```segment``` column as well as the new ```age_band``` and ```demographic``` columns
  * Generate a new ```avg_transaction``` column as the sales value divided by ```transactions``` rounded to 2 decimal places for each record

---
## Data

| Columns          | Actions to take                                                                                                                                          |
|------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| week_date        | Convert to ```DATE``` using ```CONVERT```                                                                                                                |
| week_number*     | Extract number of week using ```DATEPART```                                                                                                              |
| month_number*    | Extract month using ```DATEPART```                                                                                                                       |
| calendar_year*   | Extract year using ```DATEPART```                                                                                                                        |
| region           | No changes                                                                                                                                               |
| platform         | No changes                                                                                                                                               |
| segment          | No changes                                                                                                                                               |
| customer_type    | No changes                                                                                                                                               |
| age_band*        | Use ```CASE WHEN``` to categorize ```segment```: '1' = ```Young Adults```, '2' = ```Middle Aged```, '3' or '4' = ```Retirees``` and null = ```unknown``` |
| demographic*     | Use ```CASE WHEN``` to categorize ```segment```: 'C' = ```Couples```, 'F' = ```Families``` and null = ```unknown```                                      |
| transactions     | No changes                                                                                                                                               |
| sales            | ```CAST``` to ```bigint``` for further aggregations                                                                                                         |
| avg_transaction* | Divide ```sales``` by ```transactions``` and round up to 2 decimal places                     

```TSQL
SELECT
  CONVERT(date, week_date, 3) AS week_date,
  DATEPART(week, CONVERT(date, week_date, 3)) AS week_number,
  DATEPART(month, CONVERT(date, week_date, 3)) AS month_number,
  DATEPART(year, CONVERT(date, week_date, 3)) AS calendar_year,
  region,
  platform,
  segment,
  customer_type,
  CASE 
    WHEN RIGHT(segment, 1) = '1' THEN 'Young Adults'
    WHEN RIGHT(segment, 1) = '2' THEN 'Middle Aged'
    WHEN RIGHT(segment, 1) IN ('3', '4') THEN 'Retirees'
    ELSE 'unknown' END AS age_band,
  CASE 
    WHEN LEFT(segment, 1) = 'C' THEN 'Couples'
    WHEN LEFT(segment, 1) = 'F' THEN 'Families'
    ELSE 'unknown' END AS demographic,
  transactions,
  CAST(sales AS bigint) AS sales,
  ROUND(CAST(sales AS FLOAT)/transactions, 2) AS avg_transaction
INTO clean_weekly_sales
FROM weekly_sales;
```
The first 10 rows:
| week_date  | week_number | month_number | calendar_year | region | platform | segment | customer_type | age_band     | demographic | transactions | sales    | avg_transaction  |
|------------|-------------|--------------|---------------|--------|----------|---------|---------------|--------------|-------------|--------------|----------|------------------|
| 2020-08-31 | 36          | 8            | 2020          | ASIA   | Retail   | C3      | New           | Retirees     | Couples     | 120631       | 3656163  | 30.31            |
| 2020-08-31 | 36          | 8            | 2020          | ASIA   | Retail   | F1      | New           | Young Adults | Families    | 31574        | 996575   | 31.56            |
| 2020-08-31 | 36          | 8            | 2020          | USA    | Retail   | null    | Guest         | unknown      | unknown     | 529151       | 16509610 | 31.2             |
| 2020-08-31 | 36          | 8            | 2020          | EUROPE | Retail   | C1      | New           | Young Adults | Couples     | 4517         | 141942   | 31.42            |
| 2020-08-31 | 36          | 8            | 2020          | AFRICA | Retail   | C2      | New           | Middle Aged  | Couples     | 58046        | 1758388  | 30.29            |
| 2020-08-31 | 36          | 8            | 2020          | CANADA | Shopify  | F2      | Existing      | Middle Aged  | Families    | 1336         | 243878   | 182.54           |
| 2020-08-31 | 36          | 8            | 2020          | AFRICA | Shopify  | F3      | Existing      | Retirees     | Families    | 2514         | 519502   | 206.64           |
| 2020-08-31 | 36          | 8            | 2020          | ASIA   | Shopify  | F1      | Existing      | Young Adults | Families    | 2158         | 371417   | 172.11           |
| 2020-08-31 | 36          | 8            | 2020          | AFRICA | Shopify  | F2      | New           | Middle Aged  | Families    | 318          | 49557    | 155.84           |
| 2020-08-31 | 36          | 8            | 2020          | AFRICA | Retail   | C3      | New           | Retirees     | Couples     | 111032       | 3888162  | 35.02            |

---
## B. Data Exploration

### 1. What day of the week is used for each week_date value?
```TSQL
SELECT DISTINCT(DATENAME(dw, week_date)) AS week_date_value
FROM clean_weekly_sales;
```
| week_date_value  |
|------------------|
| Monday           |

---
### 2. What range of week numbers are missing from the dataset?
* Create a recursive CTE ```allWeeks``` to generate 52 weeks in a year
* ```LEFT JOIN``` from ```allWeeks``` to ```clean_weekly_sales```. ```NULL``` rows in ```week_number``` are missing weeks
```TSQL
WITH allWeeks AS (
  SELECT 1 AS pos
  UNION ALL
  SELECT pos+1 FROM allWeeks
  WHERE pos+1 <= 52)

SELECT 
  DISTINCT a.pos, 
  c.week_number
FROM allWeeks a
LEFT JOIN clean_weekly_sales c
  ON a.pos = c.week_number
WHERE c.week_number IS NULL
ORDER BY a.pos;
```
28 rows in total. The first 12 rows:
| pos | week_number  |
|-----|--------------|
| 1   | NULL         |
| 2   | NULL         |
| 3   | NULL         |
| 4   | NULL         |
| 5   | NULL         |
| 6   | NULL         |
| 7   | NULL         |
| 8   | NULL         |
| 9   | NULL         |
| 10  | NULL         |
| 11  | NULL         |
| 12  | NULL         |

Week 1-12 and week 37-52 are missing from the dataset.

---
### 3. How many total transactions were there for each year in the dataset?
```TSQL
SELECT 
  calendar_year,
  SUM(transactions) AS total_transactions
FROM clean_weekly_sales
GROUP BY calendar_year
ORDER BY calendar_year;
```
| calendar_year | total_transactions  |
|---------------|---------------------|
| 2018          | 346406460           |
| 2019          | 365639285           |
| 2020          | 375813651           |

---
### 4. What is the total sales for each region for each month?
```TSQL
SELECT 
  region, 
  month_number, 
  SUM(sales) AS total_sales
FROM clean_weekly_sales
GROUP BY region, month_number
ORDER BY region, month_number;
```
49 rows in total. The first 15 rows:
| region | month_number | total_sales  |
|--------|--------------|--------------|
| AFRICA | 3            | 567767480    |
| AFRICA | 4            | 1911783504   |
| AFRICA | 5            | 1647244738   |
| AFRICA | 6            | 1767559760   |
| AFRICA | 7            | 1960219710   |
| AFRICA | 8            | 1809596890   |
| AFRICA | 9            | 276320987    |
| ASIA   | 3            | 529770793    |
| ASIA   | 4            | 1804628707   |
| ASIA   | 5            | 1526285399   |
| ASIA   | 6            | 1619482889   |
| ASIA   | 7            | 1768844756   |
| ASIA   | 8            | 1663320609   |
| ASIA   | 9            | 252836807    |

---
### 5. What is the total count of transactions for each platform?
```TSQL
SELECT 
  platform,
  SUM(transactions) AS total_transactions
FROM clean_weekly_sales
GROUP BY platform;
```
| platform | total_transactions  |
|----------|---------------------|
| Retail   | 1081934227          |
| Shopify  | 5925169             |

---
### 6. What is the percentage of sales for Retail vs Shopify for each month?
```TSQL
WITH sales_cte AS (
  SELECT 
    calendar_year, 
    month_number, 
    platform, 
    SUM(sales) AS monthly_sales
  FROM clean_weekly_sales
  GROUP BY calendar_year, month_number, platform
)

SELECT 
  calendar_year, 
  month_number, 
  CAST(100.0 * MAX(CASE WHEN platform = 'Retail' THEN monthly_sales END)
	/ SUM(monthly_sales) AS decimal(5, 2)) AS pct_retail,
  CAST(100.0 * MAX(CASE WHEN platform = 'Shopify' THEN monthly_sales END)
	/ SUM(monthly_sales) AS decimal(5, 2)) AS pct_shopify
FROM sales_cte
GROUP BY calendar_year,  month_number
ORDER BY calendar_year, month_number;
```
| calendar_year | month_number | pct_retail | pct_shopify  |
|---------------|--------------|------------|--------------|
| 2018          | 3            | 97.92      | 2.08         |
| 2018          | 4            | 97.93      | 2.07         |
| 2018          | 5            | 97.73      | 2.27         |
| 2018          | 6            | 97.76      | 2.24         |
| 2018          | 7            | 97.75      | 2.25         |
| 2018          | 8            | 97.71      | 2.29         |
| 2018          | 9            | 97.68      | 2.32         |
| 2019          | 3            | 97.71      | 2.29         |
| 2019          | 4            | 97.80      | 2.20         |
| 2019          | 5            | 97.52      | 2.48         |
| 2019          | 6            | 97.42      | 2.58         |
| 2019          | 7            | 97.35      | 2.65         |
| 2019          | 8            | 97.21      | 2.79         |
| 2019          | 9            | 97.09      | 2.91         |
| 2020          | 3            | 97.30      | 2.70         |
| 2020          | 4            | 96.96      | 3.04         |
| 2020          | 5            | 96.71      | 3.29         |
| 2020          | 6            | 96.80      | 3.20         |
| 2020          | 7            | 96.67      | 3.33         |
| 2020          | 8            | 96.51      | 3.49         |

---
### 7. What is the percentage of sales by demographic for each year in the dataset?
```TSQL
WITH sales_by_demographic AS (
  SELECT 
    calendar_year,
    demographic,
    SUM(sales) AS sales
  FROM clean_weekly_sales
  GROUP BY calendar_year, demographic)

SELECT 
  calendar_year,
  CAST(100.0 * MAX(CASE WHEN demographic = 'Families' THEN sales END)
	/ SUM(sales) AS decimal(5, 2)) AS pct_families,
  CAST(100.0 * MAX(CASE WHEN demographic = 'Couples' THEN sales END) 
	/ SUM(sales) AS decimal(5, 2)) AS pct_couples,
  CAST(100.0 * MAX(CASE WHEN demographic = 'unknown' THEN sales END)
	/ SUM(sales) AS decimal(5, 2)) AS pct_unknown
FROM sales_by_demographic
GROUP BY calendar_year;
```
| calendar_year | pct_families | pct_couples | pct_unknown  |
|---------------|--------------|-------------|--------------|
| 2018          | 31.99        | 26.38       | 41.63        |
| 2019          | 32.47        | 27.28       | 40.25        |
| 2020          | 32.73        | 28.72       | 38.55        |

---
### 8. Which age_band and demographic values contribute the most to Retail sales?
```TSQL
DECLARE @retailSales bigint = (
  SELECT SUM(sales)
  FROM clean_weekly_sales
  WHERE platform = 'Retail')
				
SELECT 
  age_band,
  demographic,
  SUM(sales) AS sales,
  CAST(100.0 * SUM(sales)/@retailSales AS decimal(5, 2)) AS contribution
FROM clean_weekly_sales
WHERE platform = 'Retail'
GROUP BY age_band, demographic
ORDER BY contribution DESC;
```
| age_band     | demographic | sales       | contribution  |
|--------------|-------------|-------------|---------------|
| unknown      | unknown     | 16067285533 | 40.52         |
| Retirees     | Families    | 6634686916  | 16.73         |
| Retirees     | Couples     | 6370580014  | 16.07         |
| Middle Aged  | Families    | 4354091554  | 10.98         |
| Young Adults | Couples     | 2602922797  | 6.56          |
| Middle Aged  | Couples     | 1854160330  | 4.68          |
| Young Adults | Families    | 1770889293  | 4.47          |

The highest retail sales are contributed by *unknown* ```age_band``` and ```demographic``` at 40.52% followed by *retired families* at 16.73% and *retired couples* at 16.07%.

---
### 9. Can we use the avg_transaction column to find the average transaction size for each year for Retail vs Shopify? If not - how would you calculate it instead?
```TSQL
SELECT 
  calendar_year,
  platform,
  ROUND(AVG(avg_transaction), 0) AS avg_transaction_row,
  SUM(sales) / SUM(transactions) AS avg_transaction_group
FROM clean_weekly_sales
GROUP BY calendar_year, platform
ORDER BY calendar_year, platform;
```
| calendar_year | platform | avg_transaction_row | avg_transaction_group  |
|---------------|----------|---------------------|------------------------|
| 2018          | Retail   | 43                  | 36                     |
| 2018          | Shopify  | 188                 | 192                    |
| 2019          | Retail   | 42                  | 36                     |
| 2019          | Shopify  | 178                 | 183                    |
| 2020          | Retail   | 41                  | 36                     |
| 2020          | Shopify  | 175                 | 179                    |

What's the difference between ```avg_transaction_row``` and ```avg_transaction_group```?
* ```avg_transaction_row``` is the average transaction of each individual row in the dataset 
* ```avg_transaction_group``` is the average transaction of each ```platform``` in each ```calendar_year```

The average transaction size for each year by platform is actually ```avg_transaction_group```.

---
## C. Before & After Analysis

This technique is usually used when we inspect an important event and want to inspect the impact before and after a certain point in time. 
Taking the week_date value of 2020-06-15 as the baseline week where the Data Mart sustainable packaging changes came into effect. 
We would include all week_date values for 2020-06-15 as the start of the period after the change and the previous week_date values would be before.

Using this analysis approach - answer the following questions:

### 1. What is the total sales for the 4 weeks before and after 2020-06-15? What is the growth or reduction rate in actual values and percentage of sales?
```TSQL
--Find the week_number of '2020-06-15' (@weekNum=25)
DECLARE @weekNum int = (
  SELECT DISTINCT week_number
  FROM clean_weekly_sales
  WHERE week_date = '2020-06-15')

--Find the total sales of 4 weeks before and after @weekNum
WITH salesChanges AS (
  SELECT
    SUM(CASE WHEN week_number BETWEEN @weekNum-4 AND @weekNum-1 THEN sales END) AS before_changes,
    SUM(CASE WHEN week_number BETWEEN @weekNum AND @weekNum+3 THEN sales END) AS after_changes
  FROM clean_weekly_sales
  WHERE calendar_year = 2020
)

SELECT *,
  CAST(100.0 * (after_changes-before_changes)/before_changes AS decimal(5,2)) AS pct_change
FROM salesChanges;
```
| before_changes | after_changes | pct_change  |
|----------------|---------------|-------------|
| 2345878357     | 2318994169    | -1.15       |

---
### 2. What about the entire 12 weeks before and after?
```TSQL
--Find the week_number of '2020-06-15' (@weekNum=25)
DECLARE @weekNum int = (
  SELECT DISTINCT week_number
  FROM clean_weekly_sales
  WHERE week_date = '2020-06-15')

--Find the total sales of 12 weeks before and after @weekNum
WITH salesChanges AS (
  SELECT
    SUM(CASE WHEN week_number BETWEEN @weekNum-12 AND @weekNum-1 THEN sales END) AS before_changes,
    SUM(CASE WHEN week_number BETWEEN @weekNum AND @weekNum+11 THEN sales END) AS after_changes
  FROM clean_weekly_sales
  WHERE calendar_year = 2020
)

SELECT *,
  CAST(100.0 * (after_changes-before_changes)/before_changes AS decimal(5,2)) AS pct_change
FROM salesChanges;
```
| before_changes | after_changes | pct_change  |
|----------------|---------------|-------------|
| 7126273147     | 6973947753    | -2.14       |

---
### 3. How do the sale metrics for these 2 periods before and after compare with the previous years in 2018 and 2019?
Part 1: How do the sales metrics for 4 weeks before and after compared with the previous years in 2018 and 2019?
```TSQL
--Find the week_number of '2020-06-15' (@weekNum=25)
DECLARE @weekNum int = (
  SELECT DISTINCT week_number
  FROM clean_weekly_sales
  WHERE week_date = '2020-06-15')

--Find the total sales of 4 weeks before and after @weekNum
WITH salesChanges AS (
  SELECT
    calendar_year,
    SUM(CASE WHEN week_number BETWEEN @weekNum-3 AND @weekNum-1 THEN sales END) AS before_sales,
    SUM(CASE WHEN week_number BETWEEN @weekNum AND @weekNum+3 THEN sales END) AS after_sales
  FROM clean_weekly_sales
  GROUP BY calendar_year
)

SELECT *,
  CAST(100.0 * (after_sales-before_sales)/before_sales AS decimal(5,2)) AS pct_change
FROM salesChanges
ORDER BY calendar_year;
```
| calendar_year | before_sales | after_sales | pct_change  |
|---------------|--------------|-------------|-------------|
| 2018          | 1602763447   | 2129242914  | 32.85       |
| 2019          | 1688891616   | 2252326390  | 33.36       |
| 2020          | 1760870267   | 2318994169  | 31.70       |

Part 2: How do the sales metrics for 12 weeks before and after compared with the previous years in 2018 and 2019?
```TSQL
--Find the week_number of '2020-06-15' (@weekNum=25)
DECLARE @weekNum int = (
  SELECT DISTINCT week_number
  FROM clean_weekly_sales
  WHERE week_date = '2020-06-15')

--Find the total sales of 12 weeks before and after @weekNum
WITH salesChanges AS (
  SELECT
    calendar_year,
    SUM(CASE WHEN week_number BETWEEN @weekNum-12 AND @weekNum-1 THEN sales END) AS before_sales,
    SUM(CASE WHEN week_number BETWEEN @weekNum AND @weekNum+11 THEN sales END) AS after_sales
  FROM clean_weekly_sales
  GROUP BY calendar_year
)

SELECT *,
  CAST(100.0 * (after_sales-before_sales)/before_sales AS decimal(5,2)) AS pct_change
FROM salesChanges
ORDER BY calendar_year;
```
| calendar_year | before_sales | after_sales | pct_change  |
|---------------|--------------|-------------|-------------|
| 2018          | 6396562317   | 6500818510  | 1.63        |
| 2019          | 6883386397   | 6862646103  | -0.30       |
| 2020          | 7126273147   | 6973947753  | -2.14       |

---
## D. Bonus Question
Which areas of the business have the highest negative impact in sales metrics performance in 2020 for the 12 week before and after period?
  * ```region```
  * ```platform```
  * ```age_band```
  * ```demographic```
  * ```customer_type```
  
Do you have any further recommendations for Danny’s team at Data Mart or any interesting insights based off this analysis?

---
## Solution
First, using the technique in part C to find the ```week_number``` of ```2020-06-15```.
```TSQL
--Find the week_number of '2020-06-15' (@weekNum=25)
DECLARE @weekNum int = (
  SELECT DISTINCT week_number
  FROM clean_weekly_sales
  WHERE week_date = '2020-06-15'
  AND calendar_year =2020)
```
Then, depending on the area we want to analyze, change the column name in the ```SELECT``` and  ```GROUP BY```. 

Remember to include the ```DECLARE @weekNum``` in the beginning of each part below.

---
### 1. Sales changes by ```regions```
```TSQL
WITH regionChanges AS (
  SELECT
    region,
    SUM(CASE WHEN week_number BETWEEN @weekNum-12 AND @weekNum-1 THEN sales END) AS before_changes,
    SUM(CASE WHEN week_number BETWEEN @weekNum AND @weekNum+11 THEN sales END) AS after_changes
  FROM clean_weekly_sales
  GROUP BY region
)
SELECT *,
  CAST(100.0 * (after_changes-before_changes)/before_changes AS decimal(5,2)) AS pct_change
FROM regionChanges;
```
| region        | before_changes | after_changes | pct_change  |
|---------------|--------------|-------------|-------------|
| OCEANIA       | 6698586333   | 6640244793  | -0.87       |
| EUROPE        | 328141414    | 344420043   | 4.96        |
| SOUTH AMERICA | 611056923    | 608981392   | -0.34       |
| AFRICA        | 4942976910   | 4997516159  | 1.10        |
| CANADA        | 1244662705   | 1234025206  | -0.85       |
| ASIA          | 4613242689   | 4551927271  | -1.33       |
| USA           | 1967554887   | 1960297502  | -0.37       |

**Insights and recommendations:** 
* Overall, the sales of most countries decreased after changing packages. 
* The highest negative impact was in ```ASIA``` with -1.33%. 
Danny should reduce the number of products with sustainable packages here.
* Only ```EUROPE``` saw a significant increase of 4.96% followed by ```AFRICA``` with 1.1%. These are areas that Danny should invest more.

---
### 2. Sales changes by ```platform```
```TSQL
WITH platformChanges AS (
  SELECT
    platform,
    SUM(CASE WHEN week_number BETWEEN @weekNum-12 AND @weekNum-1 THEN sales END) AS before_changes,
    SUM(CASE WHEN week_number BETWEEN @weekNum AND @weekNum+11 THEN sales END) AS after_changes
  FROM clean_weekly_sales
  GROUP BY platform
)
SELECT *,
  CAST(100.0 * (after_changes-before_changes)/before_changes AS decimal(5,2)) AS pct_change
FROM platformChanges;
```
| platform | before_changes | after_changes | pct_change  |
|----------|--------------|-------------|-------------|
| Retail   | 19886040272  | 19768576165 | -0.59       |
| Shopify  | 520181589    | 568836201   | 9.35        |

**Insights and recommendations:** 
* ```Shopify``` stores saw an increase in sales of 9.35% while the```Retail``` stores slightly decreased by 0.59%. 
* Danny should put more products with sustanable packages in ```Shopify``` stores.

---
### 3. Sales changes by ```age_band```
```TSQL
WITH ageBandChanges AS (
  SELECT
    age_band,
    SUM(CASE WHEN week_number BETWEEN @weekNum-12 AND @weekNum-1 THEN sales END) AS before_changes,
    SUM(CASE WHEN week_number BETWEEN @weekNum AND @weekNum+11 THEN sales END) AS after_changes
  FROM clean_weekly_sales
  GROUP BY age_band
)
SELECT *,
  CAST(100.0 * (after_changes-before_changes)/before_changes AS decimal(5,2)) AS pct_change
FROM ageBandChanges;
```
| age_band     | before_changes | after_changes | pct_change  |
|--------------|--------------|-------------|-------------|
| unknown      | 8191628826   | 8146983408  | -0.55       |
| Young Adults | 2290835366   | 2285973456  | -0.21       |
| Middle Aged  | 3276892347   | 3269748622  | -0.22       |
| Retirees     | 6646865322   | 6634706880  | -0.18       |

**Insights and recommendations:** 
* Overall, the sales slightly decreased in all bands.
* ```Middle Aged``` and ```Young Adults``` had more negative impact on sales than the ```Retirees```. Those bands should not be targeted in new packages.

---
### 4. Sales changes by ```demographic```
```TSQL
WITH demographicChanges AS (
  SELECT
    demographic,
    SUM(CASE WHEN week_number BETWEEN @weekNum-12 AND @weekNum-1 THEN sales END) AS before_changes,
    SUM(CASE WHEN week_number BETWEEN @weekNum AND @weekNum+11 THEN sales END) AS after_changes
  FROM clean_weekly_sales
  GROUP BY demographic
)
SELECT *,
  CAST(100.0 * (after_changes-before_changes)/before_changes AS decimal(5,2)) AS pct_change
FROM demographicChanges;
```
| demographic | before_changes | after_changes | pct_change  |
|-------------|----------------|---------------|-------------|
| unknown     | 8191628826     | 8146983408    | -0.55       |
| Families    | 6605726904     | 6598087538    | -0.12       |
| Couples     | 5608866131     | 5592341420    | -0.29       |

**Insights and recommendations:** 
* Overall, the sales slightly decreased in all demographic groups.
* ```Couples``` had more negative impact on sales than ```Families```. Those groups should not be targeted in new packages.

---
### 5. Sales changes by ```customer_type```
```TSQL
WITH customerTypeChanges AS (
  SELECT
    customer_type,
    SUM(CASE WHEN week_number BETWEEN @weekNum-12 AND @weekNum-1 THEN sales END) AS before_changes,
    SUM(CASE WHEN week_number BETWEEN @weekNum AND @weekNum+11 THEN sales END) AS after_changes
  FROM clean_weekly_sales
  GROUP BY customer_type
)
SELECT *,
  CAST(100.0 * (after_changes-before_changes)/before_changes AS decimal(5,2)) AS pct_change
FROM customerTypeChanges;
```
| customer_type | before_changes | after_changes | pct_change  |
|---------------|----------------|---------------|-------------|
| Guest         | 7630353739     | 7595150744    | -0.46       |
| Existing      | 10168877642    | 10117367239   | -0.51       |
| New           | 2606990480     | 2624894383    | 0.69        |

**Insights and recommendations:** 
* The sales for `Guests` and `Existing` customers decreased, but increased for `New` customers.
* Further analysis should be taken to understand why `New` customers were interested in sustainable packages.
