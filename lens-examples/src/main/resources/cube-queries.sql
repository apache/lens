--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--
cube select dim1, measure2 from sample_cube where time_range_in(dt, '2014-06-25-00', '2014-06-26-00') and nontimedim = 'nonTimeDimValue2'
cube select measure2 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00')
cube select measure2 from sample_cube where time_range_in(dt, '2014-06-25-00', '2014-06-26-00')
cube select measure2 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-26-01')
cube select dim1, measure2 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00')
cube select dim1, measure2 from sample_cube where time_range_in(dt, '2014-06-25-00', '2014-06-26-00')
cube select dim1, measure2 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-26-01')
cube select dim3, measure3 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00')
cube select dim3, measure3 from sample_cube where time_range_in(dt, '2014-06-25-00', '2014-06-26-00')
cube select dim3, measure3 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-26-01')
cube select sample_dim_chain.name, measure4 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00')
cube select sample_dim_chain.name, measure4 from sample_cube where time_range_in(dt, '2014-06-25-00', '2014-06-26-00')
cube select sample_dim_chain.name, measure4 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-26-01')
cube select sample_dim_chain.name, measure4 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00') order by sample_dim_chain.name
cube select sample_dim_chain.name, measure4 from sample_cube where time_range_in(dt, '2014-06-25-00', '2014-06-26-00') order by sample_dim_chain.name
cube select sample_dim_chain.name, measure4 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-26-01') order by sample_dim_chain.name
cube select sample_dim_chain.name, measure3 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00') and sample_dim_chain.name != "first"
cube select sample_dim_chain.name, measure3 from sample_cube where time_range_in(dt, '2014-06-25-00', '2014-06-26-00') and sample_dim_chain.name != "first"
cube select sample_dim_chain.name, measure3 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-26-01') and sample_dim_chain.name != "first"
cube select sample_dim_chain.name, measure2 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00') and sample_dim_chain.name != "first" order by sample_dim_chain.name
cube select sample_dim_chain.name, measure2 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00') and sample_dim_chain.name != "first" order by sample_dim_chain.name desc
cube select sample_dim_chain.name, measure2 from sample_cube where time_range_in(dt, '2014-06-25-00', '2014-06-26-00') and sample_dim_chain.name != "first" order by sample_dim_chain.name
cube select sample_dim_chain.name, measure2 from sample_cube where time_range_in(dt, '2014-06-25-00', '2014-06-26-00') and sample_dim_chain.name != "first" order by sample_dim_chain.name desc
cube select sample_dim_chain.name, measure2 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-26-01') and sample_dim_chain.name != "first" order by sample_dim_chain.name
cube select sample_dim_chain.name, measure2 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-26-01') and sample_dim_chain.name != "first" order by sample_dim_chain.name desc
cube select sample_dim_chain.name, measure4 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00') order by sample_dim_chain.name limit 2
cube select sample_dim_chain.name, measure4 from sample_cube where time_range_in(dt, '2014-06-25-00', '2014-06-26-00') order by sample_dim_chain.name limit 2
cube select sample_dim_chain.name, measure4 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-26-01') order by sample_dim_chain.name limit 2
cube select sample_dim_chain.name, measure3 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00') and sample_dim_chain.name != "first" limit 2
cube select sample_dim_chain.name, measure3 from sample_cube where time_range_in(dt, '2014-06-25-00', '2014-06-26-00') and sample_dim_chain.name != "first" limit 2
cube select sample_dim_chain.name, measure3 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-26-01') and sample_dim_chain.name != "first" limit 2
cube select sample_dim_chain.name, measure2 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00') and sample_dim_chain.name != "first" order by sample_dim_chain.name limit 2
cube select sample_dim_chain.name, measure2 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00') and sample_dim_chain.name != "first" order by sample_dim_chain.name desc limit 2
cube select sample_dim_chain.name, measure2 from sample_cube where time_range_in(dt, '2014-06-25-00', '2014-06-26-00') and sample_dim_chain.name != "first" order by sample_dim_chain.name limit 2
cube select sample_dim_chain.name, measure2 from sample_cube where time_range_in(dt, '2014-06-25-00', '2014-06-26-00') and sample_dim_chain.name != "first" order by sample_dim_chain.name desc limit 2
cube select sample_dim_chain.name, measure2 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-26-01') and sample_dim_chain.name != "first" order by sample_dim_chain.name limit 2
cube select sample_dim_chain.name, measure2 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-26-01') and sample_dim_chain.name != "first" order by sample_dim_chain.name desc limit 2
cube select dim1, dim2, measure1, measure2 from sample_cube where time_range_in(dt, '2014-06-25-20', '2014-06-26-02')
cube select dim1, dim2, measure1, measure2 from sample_cube where time_range_in(dt, '2014-06-25-20', '2014-06-26-02') order by dim2
cube select dim1, dim2, measure1, measure2 from sample_cube where time_range_in(dt, '2014-06-25-20', '2014-06-26-02') order by dim2 desc
cube select dim1, dim2, measure1, measure2 from sample_cube where time_range_in(dt, '2014-06-25-20', '2014-06-26-02') limit 2
cube select dim1, dim2, measure1, measure2 from sample_cube where time_range_in(dt, '2014-06-25-20', '2014-06-26-02') order by dim2 limit 2
cube select dim1, dim2, measure1, measure2 from sample_cube where time_range_in(dt, '2014-06-25-20', '2014-06-26-02') order by dim2 desc limit 2
cube select dim1, avg(measure2) from sample_cube where time_range_in(dt, '2014-06-25-20', '2014-06-26-02')
cube select dim3, min(measure3) from sample_cube where time_range_in(dt, '2014-06-25-20', '2014-06-26-02')
cube select dim1, sum(measure2) from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00')
cube select dim1, sum(measure2)from sample_cube where time_range_in(dt, '2014-06-25-00', '2014-06-26-00')
cube select dim1, sum(measure2) from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-26-01')
cube select dim3, max(measure3) from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00')
cube select dim3, max(measure3) from sample_cube where time_range_in(dt, '2014-06-25-00', '2014-06-26-00')
cube select dim3, max(measure3) from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-26-01')
cube select sample_dim.name, measure4 from sample_cube join sample_dim on sample_cube.dim3=sample_dim.id where time_range_in(dt, '2014-06-24-23', '2014-06-25-00')
cube select sample_dim.name, measure4 from sample_cube left outer join sample_dim on sample_cube.dim3=sample_dim.id where time_range_in(dt, '2014-06-25-00', '2014-06-26-00')
cube select sample_dim.name, measure4 from sample_cube right outer join sample_dim on sample_cube.dim3=sample_dim.id where time_range_in(dt, '2014-06-24-23', '2014-06-26-01')
cube select sample_dim.name, measure4 from sample_cube full outer join sample_dim on sample_cube.dim3=sample_dim.id where time_range_in(dt, '2014-06-24-23', '2014-06-26-01')
select * from (cube select sample_dim.name, measure4 from sample_cube join sample_dim on sample_cube.dim3=sample_dim.id where time_range_in(dt, '2014-06-24-23', '2014-06-25-00') ) a
drop table temp1
create table temp1 as cube select sample_dim_chain.name, measure4 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00')
select * from temp1
insert overwrite local directory '/tmp/example-cube-output' cube select sample_dim_chain.name, measure4 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00')
insert overwrite local directory '/tmp/example-cube-output2' ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ('serialization.null.format'='-NA-','field.delim'=','  ) STORED AS TEXTFILE cube select sample_dim_chain.name, measure4 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00')
drop table temp2
create table temp2(name string, msr4 float)
insert overwrite table temp2 cube select sample_dim_chain.name, measure4 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00')
select * from temp2
drop table temp3
create table temp3(name string, msr4 float) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ('serialization.null.format'='-NA-','field.delim'=','  ) STORED AS TEXTFILE
insert overwrite table temp3 cube select sample_dim_chain.name, measure4 from sample_cube where time_range_in(dt, '2014-06-24-23', '2014-06-25-00')
select * from temp3
cube select product_id, store_sales from sales where time_range_in(order_time, '2015-04-11-00', '2015-04-13-00')
cube select product_id, store_sales from sales where time_range_in(order_time, '2015-04-11-00', '2015-04-13-01')
cube select product_id, store_sales from sales where time_range_in(order_time, '2015-04-12-00', '2015-04-13-00')
cube select product_id, store_sales from sales where time_range_in(order_time, '2015-04-13-00', '2015-04-13-02')
cube select product_id, store_sales from sales where time_range_in(delivery_time, '2015-04-11-00', '2015-04-13-00')
cube select product_id, store_sales from sales where time_range_in(delivery_time, '2015-04-12-00', '2015-04-13-00')
cube select promotion_sales, store_sales from sales where time_range_in(order_time, '2015-04-13-00', '2015-04-13-02')
cube select promotion_sales, store_sales from sales where time_range_in(order_time, '2015-04-13-00', '2015-04-13-01')
cube select customer_city_name, store_sales from sales where time_range_in(delivery_time, '2015-04-12-00', '2015-04-13-00')
cube select customer_city_name, store_sales from sales where time_range_in(delivery_time, '2015-04-11-00', '2015-04-13-00')
cube select customer_city_name, delivery_city.name, production_city.name, store_sales from sales where time_range_in(delivery_time, '2015-04-11-00', '2015-04-13-00')
cube select product_details.color, store_sales from sales where time_range_in(order_time, '2015-04-11-00', '2015-04-13-00') and product_details.category='Stationary'
cube select product_details.category, store_sales from sales where time_range_in(order_time, '2015-04-11-00', '2015-04-13-01')
cube select product_details.color, store_sales from sales where time_range_in(order_time, '2015-04-12-00', '2015-04-13-00')and product_details.category='Stationary'
cube select product_details.category, store_sales from sales where time_range_in(order_time, '2015-04-11-00', '2015-04-13-01')
cube select product_details.category, store_sales from sales where time_range_in(order_time, '2015-04-12-00', '2015-04-13-00')
cube select product_details.color, store_sales from sales where time_range_in(delivery_time, '2015-04-11-00', '2015-04-13-00') and product_details.category='Stationary'
cube select product_details.color, store_sales from sales where time_range_in(delivery_time, '2015-04-12-00', '2015-04-13-00') and product_details.category='Stationary'
cube select store_sales from sales where time_range_in(order_time, 'now.second-2 days', 'now.second')
-- The following query sees that ot part col is not present and falls back on querying on dt part col
cube select customer_city_name, store_cost from sales where time_range_in(order_time, '2015-04-13-03', '2015-04-13-04')
-- The following queries are for illustrating flattening of bridge table fields.
select customer_id, customer_interest, unit_sales from sales where time_range_in(order_time, '2015-04-11-00', '2015-04-13-00')
select customer_id, customer_interest, unit_sales from sales where time_range_in(order_time, '2015-04-11-00', '2015-04-13-00') and customer_interest not in ('Food')
select customer_id, customer_interest, unit_sales from sales where time_range_in(order_time, '2015-04-11-00', '2015-04-13-00') and customer_interest in ('Food')
