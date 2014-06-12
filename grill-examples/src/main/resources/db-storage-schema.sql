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



