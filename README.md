# Assumption:
1. Assuming all the `.ndjson` have correct & consistent data type
2. Edge case: Data miss-joined


# Solution 1

### Design Thinking:
1. load all data into temp tables, and then we can use pyspark sql 
2. the business quest is just the beginning of the story, so I decide to include 
   more dimension and reveal more information
3. With this sql-based solution we can use same query in other query engine (Athena/Snowflake).
Also, we only need to change sql query to adapt the requirements change.
4. If include more dimensions, we can learn more about the "dynamics" of most popular items
5. Build the right size/(aggregation dimension) of the materialised view
    - More dimensions -> more information but taking more time to render in BI
    - Less dimensions -> reduced information
    - Prepared both and see which fits the business case better
6. Sales team late may want a menu section called "most popular section" in each venue 

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

With [test_sql.yaml](configs/test/test_sql.yaml), 
output example can be found 
[here](data/output_example/sql1) 
and you can find the preview below:
```
# Output:
+------+--------+-----------+----------+-------------+-------------------+-------------------+
|  item|quantity|   venue_id|venue_name|venue_country|     venue_timezone|                dth|
+------+--------+-----------+----------+-------------+-------------------+-------------------+
|  Coke|       6|1234567890a|    Venue1|           AU|Australia/Melbourne|2021-05-18 17:00:00|
|Burger|       3|1234567890a|    Venue1|           AU|Australia/Melbourne|2021-05-18 17:00:00|
+------+--------+-----------+----------+-------------+-------------------+-------------------+
results output to [data/output/report_most_popular_item/2021-12-01T01:13:20.047049]
```

With [test_sql2.yaml](configs/test/test_sql2.yaml), 
output example can be found 
[here](data/output_example/sql2) 
and you can find the preview below:
```
+------+--------+-----------+----------+-------------+-------------------+
|  item|quantity|   venue_id|venue_name|venue_country|     venue_timezone|
+------+--------+-----------+----------+-------------+-------------------+
|  Coke|       6|1234567890a|    Venue1|           AU|Australia/Melbourne|
|Burger|       3|1234567890a|    Venue1|           AU|Australia/Melbourne|
+------+--------+-----------+----------+-------------+-------------------+
results output to [data/output/report_most_popular_item/2021-12-01T09:09:56.128183]
```

### How could analyst access the transformed table

Processed table can be saved to s3 (json|csv) and use Athena for the query,
then use tableau for visualisation

Most popular item in each venue can be found via:
```sql
-- t2 is the table built in previous steps
WITH t3 AS (
   SELECT item_id, FIRST(item) AS item, venue_id, FIRST(venue_name) AS venue_name,
   SUM(quantity) AS quantity, SUM(total_in_cents) AS total_in_cents FROM t2
   GROUP BY item_id, venue_id
),
t4 AS (
   SELECT item_id, item, venue_id, venue_name, quantity, total_in_cents,
   RANK() OVER (PARTITION BY venue_id ORDER BY quantity DESC) AS popularity_rank
   FROM t3
)
SELECT * FROM t4 WHERE popularity_rank = 1
```


Athena does not have good solution for materialised views. So in case this is a view frequently 
access we should consider materialise that in Hive or some other database for fast query.

### Thoughts
- More data enrichment can be done here (e.g. add local time, days of the week)
- A Self-service data platform can be considered, the SQL can be the only language need for DS to create reporting
- Event can be directly streaming into snowflake **internal table** (using snow pipe for data enrichment)
- s3 data lake could face performance issue when it scales up, s3's file I/O can be a problem

