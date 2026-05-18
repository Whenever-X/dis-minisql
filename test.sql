create database dist;
create table t (id int unique, age int, primary key(id));
create table o (id int unique, user_id int, amount int, primary key(id));
insert into t values (1, 10);
insert into t values (2, 20);
insert into t values (3, 30);
insert into o values (101, 1, 7);
insert into o values (102, 2, 9);
select count(*) from t;
select * from t order by age desc;
select * from t join o on t.id = o.user_id;
shard 1 select * from t;
snapshot
kill "$(tr -d '\n' < tmp/pid/node-a.pid)"
insert into t values (4, 40);
select count(*) from t;
java -jar dis-minisql/target/dis-minisql-1.0.0.jar datanode ./config/cluster3.json node-a > tmp/log/node-a.log 2>&1
echo "$!" > tmp/pid/node-a.pid
nodes
rebalance
metadata
select count(*) from t;
java -jar dis-minisql/target/dis-minisql-1.0.0.jar datanode ./config/cluster4.json node-d 
