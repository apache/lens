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

cube select id,name from sample_dim
cube select id,name from sample_dim where name != 'first'
cube select id,name from sample_dim order by name
cube select id,name from sample_dim order by name desc
cube select id,name from sample_dim where name != 'first' order by name
cube select id,name from sample_dim where name != 'first' order by name desc
cube select id,name from sample_dim limit 2
cube select id,name from sample_dim order by name limit 2
cube select id,name from sample_dim order by name desc limit 2
cube select id,name from sample_dim where name != 'first' limit 2
cube select id,name from sample_dim where name != 'first' order by name limit 2
cube select id,name from sample_dim where name != 'first' order by name desc limit 2
cube select count(id) from sample_dim
cube select count(id) from sample_dim group by name
cube select count(distinct id) from sample_dim
cube select sample_dim.name, sample_dim2_chain.name from sample_dim
cube select sample_dim.name, sample_dim2.name from sample_dim join sample_dim2 on sample_dim.d2id=sample_dim2.id
cube select sample_dim.name, sample_dim2.name from sample_dim left outer join sample_dim2 on sample_dim.d2id=sample_dim2.id
cube select sample_dim.name, sample_dim2.name from sample_dim right outer join sample_dim2 on sample_dim.d2id=sample_dim2.id
cube select sample_dim.name, sample_dim2.name from sample_dim full outer join sample_dim2 on sample_dim.d2id=sample_dim2.id
cube select count(id) from sample_dim where name != "first"
cube select count(distinct id) from sample_dim where name != "first"
cube select sample_dim.name, sample_dim2_chain.name from sample_dim where sample_dim.name != 'first'
cube select id,name from sample_db_dim
cube select id,name from sample_db_dim where name != 'first'
cube select id,name from sample_db_dim order by name
cube select id,name from sample_db_dim order by name desc
cube select id,name from sample_db_dim where name != 'first' order by name
cube select id,name from sample_db_dim where name != 'first' order by name desc
cube select id,name from sample_db_dim limit 2
cube select id,name from sample_db_dim order by name limit 2
cube select id,name from sample_db_dim order by name desc limit 2
cube select id,name from sample_db_dim where name != 'first' limit 2
cube select id,name from sample_db_dim where name != 'first' order by name limit 2
cube select id,name from sample_db_dim where name != 'first' order by name desc limit 2
cube select count(id) from sample_db_dim
cube select count(id) from sample_db_dim group by name
cube select count(distinct id) from sample_db_dim
select * from (cube select sample_dim.name name1, sample_dim2_chain.name name2 from sample_dim where sample_dim.name !='first') a
drop table temp1
create table temp1 as cube select id,name from sample_dim
select * from temp1
insert overwrite local directory '/tmp/example-output' cube select id,name from sample_dim
insert overwrite local directory '/tmp/example-output2' ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ('serialization.null.format'='-NA-','field.delim'=','  ) STORED AS TEXTFILE cube select id,name from sample_dim
drop table temp2
create table temp2(id int, name string)
insert overwrite table temp2 cube select id,name from sample_dim
select * from temp2
drop table temp3
create table temp3(id int, name string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ('serialization.null.format'='-NA-','field.delim'=','  ) STORED AS TEXTFILE
insert overwrite table temp3 cube select id,name from sample_dim
select * from temp3
cube select name from city
cube select name from city where population > 1000000
cube select name, poi from city
cube select distinct category from product
cube select id, description from product where weight > 100
cube select category, count(1) as `Number of products` from product
cube select name, customer_city_name from customer
cube select customer.name, customer_city.population from customer
