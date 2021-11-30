# Assumption:
1. Assuming all the `.ndjson` have correct & consistent data type




# Solution 1

### Design Thinking:
1. load all data into temp tables, and then we can use pyspark sql 
2. the business quest is just the beginning of the story, so I decide to push things a bit further
3. With this sql-based solution we can use same query in other query engine (Athena/Snowflake)

### Testing
Build docker and run tests
```
source scripts/testing.sh
```

### Run ETL job:

We use `.yaml` file to organise our configs, such that you can do:

```bash
# install the package and dependecies
pip3 install -e .
# The command will init ReportingEngine and execute the sql command specified in configs/test/test_sql.yaml
python3 yumr/run.py --config configs/test/test_init.yaml exec --config configs/test/test_sql.yaml
```

The config file we used in the example above can be 
found in [test_init.yaml](configs/test/test_init.yaml) 
and in [test_sql.yaml](configs/test/test_sql.yaml)

Output example can be found [here](data/output/report_most_popular_item/2021-11-30T21:35:54.006674) 
and you can find the preview below:
```
# Output:
+------+--------+-----------+-------------------+
|  item|quantity|   venue_id|                dth|
+------+--------+-----------+-------------------+
|  Coke|       6|1234567890a|2021-05-18 17:00:00|
|Burger|       3|1234567890a|2021-05-18 17:00:00|
+------+--------+-----------+-------------------+

results output to [data/output/report_most_popular_item/2021-11-30T23:42:40.850958]
```




### Thoughts
- More data enrichment can be done here (e.g. add local time days of the week)
- A Self-service data platform can be considered, the SQL can be the only language need for DS to create reporting
- Event can be directly streaming into snowflake **internal table** (using snow pipe for data enrichment)
- s3 data lake could face performance issue when it scales up, s3's file I/O can be a problem
- Athena does not have good solution for materialised views



