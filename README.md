# CDC-Framework
Description:
A Framework for implementing Change Data Capture using Spark (pyspark python API). The framework can be reused for any data source that has a key column. There are two primary data sets maintained, the History and the Mart. The Mart is based on SCD type 1 which has the lastest snapshot, where as History is based on SCD type 2. This example is demonstrated on delimited files (pipe seperated). This is also extended and reused for any datafiles as this does not depend on column definitions or header, as long as the first column in the data file is the key column. This can also be extended to any other data formats by creating Spark DataFrames. This example uses the publicly available movie lens data set. https://grouplens.org/datasets/movielens/100k/

Refer the README.rtf for more detailed steps
