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

DROP TABLE IF EXISTS mydb_dim_table3
CREATE TABLE mydb_dim_table3 (id integer, name varchar(255), detail varchar(255), d2id integer)
insert into mydb_dim_table3(id, name, detail, d2id) values (1,'first','this is one',11)
insert into mydb_dim_table3(id, name, detail, d2id) values (2,'second','this is two',12)


insert into mydb_dim_table3(id, name, detail, d2id) values (3,'third','this is three',12)

DROP TABLE IF EXISTS mydb_dim_table4
CREATE TABLE mydb_dim_table4 (id integer, name varchar(255), detail varchar(255), d2id integer)

insert into mydb_dim_table4(id, name, detail, d2id) values (1,'first','this is one',11)
insert into mydb_dim_table4(id, name, detail, d2id) values (2,'second','this is two',12)
insert into mydb_dim_table4(id, name, detail, d2id) values (3,'third','this is three',12)

DROP TABLE IF EXISTS mydb_sales_aggr_fact2
CREATE TABLE mydb_sales_aggr_fact2 (order_time timestamp, delivery_time timestamp, product_id integer, promotion_id integer, customer_city_id integer, production_city_id integer, delivery_city_id integer, unit_sales double, store_sales double, store_cost double)

INSERT INTO mydb_sales_aggr_fact2(order_time, delivery_time, product_id, promotion_id, customer_city_id, production_city_id, delivery_city_id, unit_sales, store_sales, store_cost) values ('2015-04-12 00:00:00','2015-04-12 00:00:00',1,1,1,1,1,1,5,0)
INSERT INTO mydb_sales_aggr_fact2(order_time, delivery_time, product_id, promotion_id, customer_city_id, production_city_id, delivery_city_id, unit_sales, store_sales, store_cost) values ('2015-04-12 00:00:00','2015-04-12 00:00:00',2,1,2,2,2,1,8,2)

DROP TABLE IF EXISTS mydb_sales_aggr_fact1
CREATE TABLE mydb_sales_aggr_fact1 (order_time timestamp, delivery_time timestamp, customer_id integer, product_id integer, promotion_id integer, customer_city_id integer, production_city_id integer, delivery_city_id integer, unit_sales double, store_sales double, store_cost double, max_line_item_price float, max_line_item_discount float)

INSERT INTO mydb_sales_aggr_fact1 (order_time, delivery_time, customer_id, product_id, promotion_id, customer_city_id, production_city_id, delivery_city_id, unit_sales, store_sales, store_cost, max_line_item_price, max_line_item_discount) values ('2015-04-12 00:00:00','2015-04-12 00:00:00',1,1,1,1,1,1,1,5,0,5,0)
INSERT INTO mydb_sales_aggr_fact1 (order_time, delivery_time, customer_id, product_id, promotion_id, customer_city_id, production_city_id, delivery_city_id, unit_sales, store_sales, store_cost, max_line_item_price, max_line_item_discount) values ('2015-04-12 00:00:00','2015-04-12 00:00:00',2,2,1,2,2,2,1,8,2,10,2)

DROP TABLE IF EXISTS mydb_sales_aggr_continuous_fact
CREATE TABLE mydb_sales_aggr_continuous_fact (order_time timestamp, delivery_time timestamp, customer_id integer, product_id integer, promotion_id integer, customer_city_id integer, production_city_id integer, delivery_city_id integer, unit_sales double, store_sales double, store_cost double, max_line_item_price float, max_line_item_discount float)


DROP TABLE IF EXISTS mydb_product_db_table
CREATE TABLE mydb_product_db_table (id integer, SKU_number integer, description varchar(255), color varchar(50), category varchar(255), weight float, manufacturer varchar(255))

INSERT INTO mydb_product_db_table (id, SKU_number, description, color, category, weight, manufacturer) values (1,111,'Book','White','Stationary',200,'BookCompany')
INSERT INTO mydb_product_db_table (id, SKU_number, description, color, category, weight, manufacturer) values (2,222,'Pen','Blue','Stationary',50,'BookCompany')
INSERT INTO mydb_product_db_table (id, SKU_number, description, color, category, weight, manufacturer) values (3,333,'Shirt','Purple','Clothes',200,'StylistCompany')
INSERT INTO mydb_product_db_table (id, SKU_number, description, color, category, weight, manufacturer) values (4,444,'Shoes','Blue','Wearables',1000,'StylistCompany')
INSERT INTO mydb_product_db_table (id, SKU_number, description, color, category, weight, manufacturer) values (5,555,'Chocolates','Brown','Food',500,'ChocoManufacturer')

DROP TABLE IF EXISTS mydb_customer_table
CREATE TABLE mydb_customer_table (id integer, name varchar(255), gender varchar(50), age integer, city_id integer, customer_credit_status varchar(255))

INSERT INTO mydb_customer_table (id, name, gender, age, city_id, customer_credit_status) values (1,'Ramu','Male',25,1,'Good')
INSERT INTO mydb_customer_table (id, name, gender, age, city_id, customer_credit_status) values (2,'Meena','Female',30,2,'Good')
INSERT INTO mydb_customer_table (id, name, gender, age, city_id, customer_credit_status) values (3,'JohnX','Male',25,3,'Bad')
INSERT INTO mydb_customer_table (id, name, gender, age, city_id, customer_credit_status) values (4,'Anju','Female',35,4,'Good')

DROP TABLE IF EXISTS mydb_city_subset
CREATE TABLE mydb_city_subset (id integer, name varchar(255))

INSERT INTO mydb_city_subset (id, name) values (1, 'Bangalore')
INSERT INTO mydb_city_subset (id, name) values (2, 'Hyderabad')
INSERT INTO mydb_city_subset (id, name) values (3, 'Austin')
INSERT INTO mydb_city_subset (id, name) values (4, 'San Fransisco')

DROP TABLE IF EXISTS mydb_customer_interests_table
CREATE TABLE mydb_customer_interests_table (customer_id integer, interest_id integer)

INSERT INTO mydb_customer_interests_table (customer_id, interest_id) values (1,1)
INSERT INTO mydb_customer_interests_table (customer_id, interest_id) values (1,2)
INSERT INTO mydb_customer_interests_table (customer_id, interest_id) values (3,1)
INSERT INTO mydb_customer_interests_table (customer_id, interest_id) values (4,1)
INSERT INTO mydb_customer_interests_table (customer_id, interest_id) values (4,1)
INSERT INTO mydb_customer_interests_table (customer_id, interest_id) values (4,3)

DROP TABLE IF EXISTS mydb_interests_table
CREATE TABLE mydb_interests_table (id integer, name varchar(255))

INSERT INTO mydb_interests_table (id, name) values (1,'Food')
INSERT INTO mydb_interests_table (id, name) values (2,'Fashion')
INSERT INTO mydb_interests_table (id, name) values (3,'Furniture')
INSERT INTO mydb_interests_table (id, name) values (4,'Electronics')
