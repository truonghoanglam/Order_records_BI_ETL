# ETL Pipeline for Order Records BI System 

![Screenshot 2023-01-05 140411](https://user-images.githubusercontent.com/15308273/210721461-f4f74f6b-fbf2-4476-828d-3b2d7fbe5a21.png)


This is an ETL pipeline to extract data from Rest API of an Order records system. You can also modify it and apply it to other platforms. Just make sure to read API doc of the platform first.

## 1 - Extract 
- Read API doc
- Use Request library to extract data, loop through every page and parse into JSON
- Add all the data into a big dataframe

## 2 - Transform
- Remove duplicate
- Extract only needed columns
- Transform data and do feature engineering

## 3 - Load
- Connect to PostgreSQL by using psycopg2 library
- Devide the dataframe into many small one, and load one by one
- Use Incremental load and Full load approaches

## 4 - Use SQL to extract data from database and use BI tool to visualize data
- Query data from database, create a big data table
- Create a cache or data model on BI tool to reduce loading time
