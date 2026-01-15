
 
CREATE TABLE tab1 AS
    FROM read_csv([
        'https://blobs.duckdb.org/nl-railway/services-2025-03.csv.gz',
        'https://blobs.duckdb.org/nl-railway/services-2025-04.csv.gz',
        'https://blobs.duckdb.org/nl-railway/services-2025-05.csv.gz',
        'https://blobs.duckdb.org/nl-railway/services-2025-06.csv.gz',
        'https://blobs.duckdb.org/nl-railway/services-2025-07.csv.gz',
         'https://blobs.duckdb.org/nl-railway/services-2025-08.csv.gz'
         ]); 


summarize select * from tab1;


select count(*) from tab1;

create table tab2 as select * from tab1;
create table tab3 as select * from tab1;
create table tab4 as select * from tab1;
create table tab5 as select * from tab1;
create table tab6 as select * from tab1;
create table tab7 as select * from tab1;
create table tab8 as select * from tab1;
create table tab9 as select * from tab1;
create table tab10 as select * from tab1; 


create table join_all AS 
  from tab1
  positional join tab2
  positional join tab3
  positional join tab4
  positional join tab5
  positional join tab6
  positional join tab7
  positional join tab8
  positional join tab9
  positional join tab10
  ;
 

summarize select * from join_all;
 
select count(*) from join_all;

explain analyze
from tab1
  positional join tab2
  positional join tab3
  positional join tab4
  positional join tab5
  positional join tab6
  positional join tab7
  positional join tab8
  positional join tab9
  positional join tab10
  ;

 

explain analyze 
  from tab1
  natural join tab2
  natural join tab3
  natural join tab4
  natural join tab5
  natural join tab6
  natural join tab7
  natural join tab8
  natural join tab9
  natural join tab10;
 
