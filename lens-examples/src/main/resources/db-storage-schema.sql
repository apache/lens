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



