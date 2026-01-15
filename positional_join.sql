
 
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
100% ▕██████████████████████████████████████▏ (00:00:49.37 elapsed)     
┌─────────────────────────────────────┐
│┌───────────────────────────────────┐│
││    Query Profiling Information    ││
│└───────────────────────────────────┘│
└─────────────────────────────────────┘
explain analyze from tab1 positional join tab2 positional join tab3 positional join tab4 positional join tab5 positional join tab6 positional join tab7 positional join tab8 positional join tab9 positional join tab10 ;
┌─────────────────────────────────────┐
│┌───────────────────────────────────┐│
││         HTTPFS HTTP Stats         ││
││                                   ││
││            in: 0 bytes            ││
││            out: 0 bytes           ││
││              #HEAD: 0             ││
││              #GET: 0              ││
││              #PUT: 0              ││
││              #POST: 0             ││
││             #DELETE: 0            ││
│└───────────────────────────────────┘│
└─────────────────────────────────────┘
┌────────────────────────────────────────────────┐
│┌──────────────────────────────────────────────┐│
││              Total Time: 49.40s              ││
│└──────────────────────────────────────────────┘│
└────────────────────────────────────────────────┘
┌─────────────────────┐
│        QUERY        │
└──────────┬──────────┘
┌──────────┴──────────┐
│   EXPLAIN_ANALYZE   │
│    ──────────────   │
│        0 rows       │
│       (0.00s)       │
└──────────┬──────────┘
┌──────────┴──────────┐
│   POSITIONAL_SCAN   │
│    ──────────────   │
│   11,148,373 rows   ├───────────┬──────────────────────┬──────────────────────┬──────────────────────┬──────────────────────┬──────────────────────┬──────────────────────┬──────────────────────┬──────────────────────┐
│       (49.23s)      │           │                      │                      │                      │                      │                      │                      │                      │                      │
└──────────┬──────────┘           │                      │                      │                      │                      │                      │                      │                      │                      │
┌──────────┴──────────┐┌──────────┴──────────┐┌──────────┴──────────┐┌──────────┴──────────┐┌──────────┴──────────┐┌──────────┴──────────┐┌──────────┴──────────┐┌──────────┴──────────┐┌──────────┴──────────┐┌──────────┴──────────┐
│      TABLE_SCAN     ││      TABLE_SCAN     ││      TABLE_SCAN     ││      TABLE_SCAN     ││      TABLE_SCAN     ││      TABLE_SCAN     ││      TABLE_SCAN     ││      TABLE_SCAN     ││      TABLE_SCAN     ││      TABLE_SCAN     │
│    ──────────────   ││    ──────────────   ││    ──────────────   ││    ──────────────   ││    ──────────────   ││    ──────────────   ││    ──────────────   ││    ──────────────   ││    ──────────────   ││    ──────────────   │
│     Table: tab1     ││     Table: tab2     ││     Table: tab3     ││     Table: tab4     ││     Table: tab5     ││     Table: tab6     ││     Table: tab7     ││     Table: tab8     ││     Table: tab9     ││     Table: tab10    │
│                     ││                     ││                     ││                     ││                     ││                     ││                     ││                     ││                     ││                     │
│        Type:        ││        Type:        ││        Type:        ││        Type:        ││        Type:        ││        Type:        ││        Type:        ││        Type:        ││        Type:        ││        Type:        │
│   Sequential Scan   ││   Sequential Scan   ││   Sequential Scan   ││   Sequential Scan   ││   Sequential Scan   ││   Sequential Scan   ││   Sequential Scan   ││   Sequential Scan   ││   Sequential Scan   ││   Sequential Scan   │
│                     ││                     ││                     ││                     ││                     ││                     ││                     ││                     ││                     ││                     │
│     Projections:    ││     Projections:    ││     Projections:    ││     Projections:    ││     Projections:    ││     Projections:    ││     Projections:    ││     Projections:    ││     Projections:    ││     Projections:    │
│    Service:RDT-ID   ││    Service:RDT-ID   ││    Service:RDT-ID   ││    Service:RDT-ID   ││    Service:RDT-ID   ││    Service:RDT-ID   ││    Service:RDT-ID   ││    Service:RDT-ID   ││    Service:RDT-ID   ││    Service:RDT-ID   │
│     Service:Date    ││     Service:Date    ││     Service:Date    ││     Service:Date    ││     Service:Date    ││     Service:Date    ││     Service:Date    ││     Service:Date    ││     Service:Date    ││     Service:Date    │
│     Service:Type    ││     Service:Type    ││     Service:Type    ││     Service:Type    ││     Service:Type    ││     Service:Type    ││     Service:Type    ││     Service:Type    ││     Service:Type    ││     Service:Type    │
│   Service:Company   ││   Service:Company   ││   Service:Company   ││   Service:Company   ││   Service:Company   ││   Service:Company   ││   Service:Company   ││   Service:Company   ││   Service:Company   ││   Service:Company   │
│ Service:Train number││ Service:Train number││ Service:Train number││ Service:Train number││ Service:Train number││ Service:Train number││ Service:Train number││ Service:Train number││ Service:Train number││ Service:Train number│
│  Service:Completely ││  Service:Completely ││  Service:Completely ││  Service:Completely ││  Service:Completely ││  Service:Completely ││  Service:Completely ││  Service:Completely ││  Service:Completely ││  Service:Completely │
│       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     │
│    Service:Partly   ││    Service:Partly   ││    Service:Partly   ││    Service:Partly   ││    Service:Partly   ││    Service:Partly   ││    Service:Partly   ││    Service:Partly   ││    Service:Partly   ││    Service:Partly   │
│       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     │
│Service:Maximum delay││Service:Maximum delay││Service:Maximum delay││Service:Maximum delay││Service:Maximum delay││Service:Maximum delay││Service:Maximum delay││Service:Maximum delay││Service:Maximum delay││Service:Maximum delay│
│     Stop:RDT-ID     ││     Stop:RDT-ID     ││     Stop:RDT-ID     ││     Stop:RDT-ID     ││     Stop:RDT-ID     ││     Stop:RDT-ID     ││     Stop:RDT-ID     ││     Stop:RDT-ID     ││     Stop:RDT-ID     ││     Stop:RDT-ID     │
│  Stop:Station code  ││  Stop:Station code  ││  Stop:Station code  ││  Stop:Station code  ││  Stop:Station code  ││  Stop:Station code  ││  Stop:Station code  ││  Stop:Station code  ││  Stop:Station code  ││  Stop:Station code  │
│  Stop:Station name  ││  Stop:Station name  ││  Stop:Station name  ││  Stop:Station name  ││  Stop:Station name  ││  Stop:Station name  ││  Stop:Station name  ││  Stop:Station name  ││  Stop:Station name  ││  Stop:Station name  │
│  Stop:Arrival time  ││  Stop:Arrival time  ││  Stop:Arrival time  ││  Stop:Arrival time  ││  Stop:Arrival time  ││  Stop:Arrival time  ││  Stop:Arrival time  ││  Stop:Arrival time  ││  Stop:Arrival time  ││  Stop:Arrival time  │
│  Stop:Arrival delay ││  Stop:Arrival delay ││  Stop:Arrival delay ││  Stop:Arrival delay ││  Stop:Arrival delay ││  Stop:Arrival delay ││  Stop:Arrival delay ││  Stop:Arrival delay ││  Stop:Arrival delay ││  Stop:Arrival delay │
│     Stop:Arrival    ││     Stop:Arrival    ││     Stop:Arrival    ││     Stop:Arrival    ││     Stop:Arrival    ││     Stop:Arrival    ││     Stop:Arrival    ││     Stop:Arrival    ││     Stop:Arrival    ││     Stop:Arrival    │
│       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     │
│ Stop:Departure time ││ Stop:Departure time ││ Stop:Departure time ││ Stop:Departure time ││ Stop:Departure time ││ Stop:Departure time ││ Stop:Departure time ││ Stop:Departure time ││ Stop:Departure time ││ Stop:Departure time │
│ Stop:Departure delay││ Stop:Departure delay││ Stop:Departure delay││ Stop:Departure delay││ Stop:Departure delay││ Stop:Departure delay││ Stop:Departure delay││ Stop:Departure delay││ Stop:Departure delay││ Stop:Departure delay│
│    Stop:Departure   ││    Stop:Departure   ││    Stop:Departure   ││    Stop:Departure   ││    Stop:Departure   ││    Stop:Departure   ││    Stop:Departure   ││    Stop:Departure   ││    Stop:Departure   ││    Stop:Departure   │
│       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     ││       cancelled     │
│                     ││                     ││                     ││                     ││                     ││                     ││                     ││                     ││                     ││                     │
│        0 rows       ││        0 rows       ││        0 rows       ││        0 rows       ││        0 rows       ││        0 rows       ││        0 rows       ││        0 rows       ││        0 rows       ││        0 rows       │
│       (0.00s)       ││       (0.00s)       ││       (0.00s)       ││       (0.00s)       ││       (0.00s)       ││       (0.00s)       ││       (0.00s)       ││       (0.00s)       ││       (0.00s)       ││       (0.00s)       │
└─────────────────────┘└─────────────────────┘└─────────────────────┘└─────────────────────┘└─────────────────────┘└─────────────────────┘└─────────────────────┘└─────────────────────┘└─────────────────────┘└─────────────────────┘
D 
 



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

100% ▕██████████████████████████████████████▏ (00:00:58.37 elapsed)     
┌─────────────────────────────────────┐
│┌───────────────────────────────────┐│
││    Query Profiling Information    ││
│└───────────────────────────────────┘│
└─────────────────────────────────────┘
explain analyze   from tab1   natural join tab2   natural join tab3   natural join tab4   natural join tab5   natural join tab6   natural join tab7   natural join tab8   natural join tab9   natural join tab10   ;
┌─────────────────────────────────────┐
│┌───────────────────────────────────┐│
││         HTTPFS HTTP Stats         ││
││                                   ││
││            in: 0 bytes            ││
││            out: 0 bytes           ││
││              #HEAD: 0             ││
││              #GET: 0              ││
││              #PUT: 0              ││
││              #POST: 0             ││
││             #DELETE: 0            ││
│└───────────────────────────────────┘│
└─────────────────────────────────────┘
┌────────────────────────────────────────────────┐
│┌──────────────────────────────────────────────┐│
││              Total Time: 58.48s              ││
│└──────────────────────────────────────────────┘│
└────────────────────────────────────────────────┘
┌─────────────────────┐
│        QUERY        │
└──────────┬──────────┘
┌──────────┴──────────┐
│   EXPLAIN_ANALYZE   │
│    ──────────────   │
│        0 rows       │
│       (0.00s)       │
└──────────┬──────────┘
┌──────────┴──────────┐
│      PROJECTION     │
│    ──────────────   │
│    Service:RDT-ID   │
│     Service:Date    │
│     Service:Type    │
│   Service:Company   │
│ Service:Train number│
│  Service:Completely │
│       cancelled     │
│    Service:Partly   │
│       cancelled     │
│Service:Maximum delay│
│     Stop:RDT-ID     │
│  Stop:Station code  │
│  Stop:Station name  │
│  Stop:Arrival time  │
│  Stop:Arrival delay │
│     Stop:Arrival    │
│       cancelled     │
│ Stop:Departure time │
│ Stop:Departure delay│
│    Stop:Departure   │
│       cancelled     │
│                     │
│    8,684,407 rows   │
│       (0.02s)       │
└──────────┬──────────┘
┌──────────┴──────────┐
│      HASH_JOIN      │
│    ──────────────   │
│      Join Type:     │
│        INNER        │
│                     │
│     Conditions:     │
│    Stop:Departure   │
│   cancelled = Stop  │
│ :Departure cancelled│
│ Stop:Departure delay│
│   = Stop:Departure  │
│         delay       │
│     Stop:Arrival    │
│   cancelled = Stop  │
│  :Arrival cancelled │
│ Stop:Arrival time = │
│   Stop:Arrival time │
│ Service:Train number│
│    = Service:Train  │
│        number       │
│  Stop:RDT-ID = Stop │
│       :RDT-ID       │
│ Stop:Departure time │
│= Stop:Departure time│
│   Service:Maximum   │
│    delay = Service  ├───────────┐
│    :Maximum delay   │           │
│    Service:Partly   │           │
│  cancelled = Service│           │
│  :Partly cancelled  │           │
│  Service:Completely │           │
│  cancelled = Service│           │
│:Completely cancelled│           │
│ Stop:Station code = │           │
│   Stop:Station code │           │
│    Service:Date =   │           │
│     Service:Date    │           │
│ Stop:Arrival delay =│           │
│  Stop:Arrival delay │           │
│  Service:Company =  │           │
│    Service:Company  │           │
│ Stop:Station name = │           │
│   Stop:Station name │           │
│    Service:Type =   │           │
│     Service:Type    │           │
│   Service:RDT-ID =  │           │
│    Service:RDT-ID   │           │
│                     │           │
│    8,684,407 rows   │           │
│      (101.76s)      │           │
└──────────┬──────────┘           │
┌──────────┴──────────┐┌──────────┴──────────┐
│      TABLE_SCAN     ││      HASH_JOIN      │
│    ──────────────   ││    ──────────────   │
│     Table: tab1     ││      Join Type:     │
│                     ││        INNER        │
│        Type:        ││                     │
│   Sequential Scan   ││     Conditions:     │
│                     ││    Stop:Departure   │
│     Projections:    ││   cancelled = Stop  │
│    Stop:Departure   ││ :Departure cancelled│
│       cancelled     ││ Stop:Departure delay│
│ Stop:Departure delay││   = Stop:Departure  │
│     Stop:Arrival    ││         delay       │
│       cancelled     ││     Stop:Arrival    │
│  Stop:Arrival time  ││   cancelled = Stop  │
│ Service:Train number││  :Arrival cancelled │
│     Stop:RDT-ID     ││ Stop:Arrival time = │
│ Stop:Departure time ││   Stop:Arrival time │
│Service:Maximum delay││ Service:Train number│
│    Service:Partly   ││    = Service:Train  │
│       cancelled     ││        number       │
│  Service:Completely ││  Stop:RDT-ID = Stop │
│       cancelled     ││       :RDT-ID       │
│  Stop:Station code  ││ Stop:Departure time │
│     Service:Date    ││= Stop:Departure time│
│  Stop:Arrival delay ││   Service:Maximum   │
│   Service:Company   ││    delay = Service  ├──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  Stop:Station name  ││    :Maximum delay   │                                                                                                                              │
│     Service:Type    ││    Service:Partly   │                                                                                                                              │
│    Service:RDT-ID   ││  cancelled = Service│                                                                                                                              │
│                     ││  :Partly cancelled  │                                                                                                                              │
│                     ││  Service:Completely │                                                                                                                              │
│                     ││  cancelled = Service│                                                                                                                              │
│                     ││:Completely cancelled│                                                                                                                              │
│                     ││ Stop:Station code = │                                                                                                                              │
│                     ││   Stop:Station code │                                                                                                                              │
│                     ││    Service:Date =   │                                                                                                                              │
│                     ││     Service:Date    │                                                                                                                              │
│                     ││ Stop:Arrival delay =│                                                                                                                              │
│                     ││  Stop:Arrival delay │                                                                                                                              │
│                     ││  Service:Company =  │                                                                                                                              │
│                     ││    Service:Company  │                                                                                                                              │
│                     ││ Stop:Station name = │                                                                                                                              │
│                     ││   Stop:Station name │                                                                                                                              │
│                     ││    Service:Type =   │                                                                                                                              │
│                     ││     Service:Type    │                                                                                                                              │
│                     ││   Service:RDT-ID =  │                                                                                                                              │
│                     ││    Service:RDT-ID   │                                                                                                                              │
│                     ││                     │                                                                                                                              │
│    8,684,407 rows   ││    8,684,407 rows   │                                                                                                                              │
│       (6.84s)       ││       (28.36s)      │                                                                                                                              │
└─────────────────────┘└──────────┬──────────┘                                                                                                                              │
                       ┌──────────┴──────────┐                                                                                                                   ┌──────────┴──────────┐
                       │      HASH_JOIN      │                                                                                                                   │      HASH_JOIN      │
                       │    ──────────────   │                                                                                                                   │    ──────────────   │
                       │      Join Type:     │                                                                                                                   │      Join Type:     │
                       │        INNER        │                                                                                                                   │        INNER        │
                       │                     │                                                                                                                   │                     │
                       │     Conditions:     │                                                                                                                   │     Conditions:     │
                       │    Stop:Departure   │                                                                                                                   │    Stop:Departure   │
                       │   cancelled = Stop  │                                                                                                                   │   cancelled = Stop  │
                       │ :Departure cancelled│                                                                                                                   │ :Departure cancelled│
                       │ Stop:Departure delay│                                                                                                                   │ Stop:Departure delay│
                       │   = Stop:Departure  │                                                                                                                   │   = Stop:Departure  │
                       │         delay       │                                                                                                                   │         delay       │
                       │     Stop:Arrival    │                                                                                                                   │     Stop:Arrival    │
                       │   cancelled = Stop  │                                                                                                                   │   cancelled = Stop  │
                       │  :Arrival cancelled │                                                                                                                   │  :Arrival cancelled │
                       │ Stop:Arrival time = │                                                                                                                   │ Stop:Arrival time = │
                       │   Stop:Arrival time │                                                                                                                   │   Stop:Arrival time │
                       │ Service:Train number│                                                                                                                   │ Service:Train number│
                       │    = Service:Train  │                                                                                                                   │    = Service:Train  │
                       │        number       │                                                                                                                   │        number       │
                       │  Stop:RDT-ID = Stop │                                                                                                                   │  Stop:RDT-ID = Stop │
                       │       :RDT-ID       │                                                                                                                   │       :RDT-ID       │
                       │ Stop:Departure time │                                                                                                                   │ Stop:Departure time │
                       │= Stop:Departure time│                                                                                                                   │= Stop:Departure time│
                       │   Service:Maximum   │                                                                                                                   │   Service:Maximum   │
                       │    delay = Service  ├────────────────────────────────────────────────────────────────────────────────┐                                  │    delay = Service  ├───────────┐
                       │    :Maximum delay   │                                                                                │                                  │    :Maximum delay   │           │
                       │    Service:Partly   │                                                                                │                                  │    Service:Partly   │           │
                       │  cancelled = Service│                                                                                │                                  │  cancelled = Service│           │
                       │  :Partly cancelled  │                                                                                │                                  │  :Partly cancelled  │           │
                       │  Service:Completely │                                                                                │                                  │  Service:Completely │           │
                       │  cancelled = Service│                                                                                │                                  │  cancelled = Service│           │
                       │:Completely cancelled│                                                                                │                                  │:Completely cancelled│           │
                       │ Stop:Station code = │                                                                                │                                  │ Stop:Station code = │           │
                       │   Stop:Station code │                                                                                │                                  │   Stop:Station code │           │
                       │    Service:Date =   │                                                                                │                                  │    Service:Date =   │           │
                       │     Service:Date    │                                                                                │                                  │     Service:Date    │           │
                       │ Stop:Arrival delay =│                                                                                │                                  │ Stop:Arrival delay =│           │
                       │  Stop:Arrival delay │                                                                                │                                  │  Stop:Arrival delay │           │
                       │  Service:Company =  │                                                                                │                                  │  Service:Company =  │           │
                       │    Service:Company  │                                                                                │                                  │    Service:Company  │           │
                       │ Stop:Station name = │                                                                                │                                  │ Stop:Station name = │           │
                       │   Stop:Station name │                                                                                │                                  │   Stop:Station name │           │
                       │    Service:Type =   │                                                                                │                                  │    Service:Type =   │           │
                       │     Service:Type    │                                                                                │                                  │     Service:Type    │           │
                       │   Service:RDT-ID =  │                                                                                │                                  │   Service:RDT-ID =  │           │
                       │    Service:RDT-ID   │                                                                                │                                  │    Service:RDT-ID   │           │
                       │                     │                                                                                │                                  │                     │           │
                       │    8,684,407 rows   │                                                                                │                                  │    8,684,407 rows   │           │
                       │       (34.42s)      │                                                                                │                                  │       (21.28s)      │           │
                       └──────────┬──────────┘                                                                                │                                  └──────────┬──────────┘           │
                       ┌──────────┴──────────┐                                                                     ┌──────────┴──────────┐                       ┌──────────┴──────────┐┌──────────┴──────────┐
                       │      HASH_JOIN      │                                                                     │      HASH_JOIN      │                       │      TABLE_SCAN     ││      HASH_JOIN      │
                       │    ──────────────   │                                                                     │    ──────────────   │                       │    ──────────────   ││    ──────────────   │
                       │      Join Type:     │                                                                     │      Join Type:     │                       │     Table: tab2     ││      Join Type:     │
                       │        INNER        │                                                                     │        INNER        │                       │                     ││        INNER        │
                       │                     │                                                                     │                     │                       │        Type:        ││                     │
                       │     Conditions:     │                                                                     │     Conditions:     │                       │   Sequential Scan   ││     Conditions:     │
                       │    Stop:Departure   │                                                                     │    Stop:Departure   │                       │                     ││    Stop:Departure   │
                       │   cancelled = Stop  │                                                                     │   cancelled = Stop  │                       │     Projections:    ││   cancelled = Stop  │
                       │ :Departure cancelled│                                                                     │ :Departure cancelled│                       │    Stop:Departure   ││ :Departure cancelled│
                       │ Stop:Departure delay│                                                                     │ Stop:Departure delay│                       │       cancelled     ││ Stop:Departure delay│
                       │   = Stop:Departure  │                                                                     │   = Stop:Departure  │                       │ Stop:Departure delay││   = Stop:Departure  │
                       │         delay       │                                                                     │         delay       │                       │     Stop:Arrival    ││         delay       │
                       │     Stop:Arrival    │                                                                     │     Stop:Arrival    │                       │       cancelled     ││     Stop:Arrival    │
                       │   cancelled = Stop  │                                                                     │   cancelled = Stop  │                       │  Stop:Arrival time  ││   cancelled = Stop  │
                       │  :Arrival cancelled │                                                                     │  :Arrival cancelled │                       │ Service:Train number││  :Arrival cancelled │
                       │ Stop:Arrival time = │                                                                     │ Stop:Arrival time = │                       │     Stop:RDT-ID     ││ Stop:Arrival time = │
                       │   Stop:Arrival time │                                                                     │   Stop:Arrival time │                       │ Stop:Departure time ││   Stop:Arrival time │
                       │ Service:Train number│                                                                     │ Service:Train number│                       │Service:Maximum delay││ Service:Train number│
                       │    = Service:Train  │                                                                     │    = Service:Train  │                       │    Service:Partly   ││    = Service:Train  │
                       │        number       │                                                                     │        number       │                       │       cancelled     ││        number       │
                       │  Stop:RDT-ID = Stop │                                                                     │  Stop:RDT-ID = Stop │                       │  Service:Completely ││  Stop:RDT-ID = Stop │
                       │       :RDT-ID       │                                                                     │       :RDT-ID       │                       │       cancelled     ││       :RDT-ID       │
                       │ Stop:Departure time │                                                                     │ Stop:Departure time │                       │  Stop:Station code  ││ Stop:Departure time │
                       │= Stop:Departure time│                                                                     │= Stop:Departure time│                       │     Service:Date    ││= Stop:Departure time│
                       │   Service:Maximum   │                                                                     │   Service:Maximum   │                       │  Stop:Arrival delay ││   Service:Maximum   │
                       │    delay = Service  ├───────────┐                                                         │    delay = Service  ├───────────┐           │   Service:Company   ││    delay = Service  ├───────────┐
                       │    :Maximum delay   │           │                                                         │    :Maximum delay   │           │           │  Stop:Station name  ││    :Maximum delay   │           │
                       │    Service:Partly   │           │                                                         │    Service:Partly   │           │           │     Service:Type    ││    Service:Partly   │           │
                       │  cancelled = Service│           │                                                         │  cancelled = Service│           │           │    Service:RDT-ID   ││  cancelled = Service│           │
                       │  :Partly cancelled  │           │                                                         │  :Partly cancelled  │           │           │                     ││  :Partly cancelled  │           │
                       │  Service:Completely │           │                                                         │  Service:Completely │           │           │                     ││  Service:Completely │           │
                       │  cancelled = Service│           │                                                         │  cancelled = Service│           │           │                     ││  cancelled = Service│           │
                       │:Completely cancelled│           │                                                         │:Completely cancelled│           │           │                     ││:Completely cancelled│           │
                       │ Stop:Station code = │           │                                                         │ Stop:Station code = │           │           │                     ││ Stop:Station code = │           │
                       │   Stop:Station code │           │                                                         │   Stop:Station code │           │           │                     ││   Stop:Station code │           │
                       │    Service:Date =   │           │                                                         │    Service:Date =   │           │           │                     ││    Service:Date =   │           │
                       │     Service:Date    │           │                                                         │     Service:Date    │           │           │                     ││     Service:Date    │           │
                       │ Stop:Arrival delay =│           │                                                         │ Stop:Arrival delay =│           │           │                     ││ Stop:Arrival delay =│           │
                       │  Stop:Arrival delay │           │                                                         │  Stop:Arrival delay │           │           │                     ││  Stop:Arrival delay │           │
                       │  Service:Company =  │           │                                                         │  Service:Company =  │           │           │                     ││  Service:Company =  │           │
                       │    Service:Company  │           │                                                         │    Service:Company  │           │           │                     ││    Service:Company  │           │
                       │ Stop:Station name = │           │                                                         │ Stop:Station name = │           │           │                     ││ Stop:Station name = │           │
                       │   Stop:Station name │           │                                                         │   Stop:Station name │           │           │                     ││   Stop:Station name │           │
                       │    Service:Type =   │           │                                                         │    Service:Type =   │           │           │                     ││    Service:Type =   │           │
                       │     Service:Type    │           │                                                         │     Service:Type    │           │           │                     ││     Service:Type    │           │
                       │   Service:RDT-ID =  │           │                                                         │   Service:RDT-ID =  │           │           │                     ││   Service:RDT-ID =  │           │
                       │    Service:RDT-ID   │           │                                                         │    Service:RDT-ID   │           │           │                     ││    Service:RDT-ID   │           │
                       │                     │           │                                                         │                     │           │           │                     ││                     │           │
                       │    8,684,407 rows   │           │                                                         │    8,684,407 rows   │           │           │    8,684,407 rows   ││    8,684,407 rows   │           │
                       │       (62.49s)      │           │                                                         │       (7.83s)       │           │           │       (9.98s)       ││       (12.43s)      │           │
                       └──────────┬──────────┘           │                                                         └──────────┬──────────┘           │           └─────────────────────┘└──────────┬──────────┘           │
                       ┌──────────┴──────────┐┌──────────┴──────────┐                                              ┌──────────┴──────────┐┌──────────┴──────────┐                       ┌──────────┴──────────┐┌──────────┴──────────┐
                       │      TABLE_SCAN     ││      HASH_JOIN      │                                              │      TABLE_SCAN     ││      TABLE_SCAN     │                       │      TABLE_SCAN     ││      TABLE_SCAN     │
                       │    ──────────────   ││    ──────────────   │                                              │    ──────────────   ││    ──────────────   │                       │    ──────────────   ││    ──────────────   │
                       │     Table: tab7     ││      Join Type:     │                                              │     Table: tab5     ││     Table: tab6     │                       │     Table: tab3     ││     Table: tab4     │
                       │                     ││        INNER        │                                              │                     ││                     │                       │                     ││                     │
                       │        Type:        ││                     │                                              │        Type:        ││        Type:        │                       │        Type:        ││        Type:        │
                       │   Sequential Scan   ││     Conditions:     │                                              │   Sequential Scan   ││   Sequential Scan   │                       │   Sequential Scan   ││   Sequential Scan   │
                       │                     ││    Stop:Departure   │                                              │                     ││                     │                       │                     ││                     │
                       │     Projections:    ││   cancelled = Stop  │                                              │     Projections:    ││     Projections:    │                       │     Projections:    ││     Projections:    │
                       │    Stop:Departure   ││ :Departure cancelled│                                              │    Stop:Departure   ││    Stop:Departure   │                       │    Stop:Departure   ││    Stop:Departure   │
                       │       cancelled     ││ Stop:Departure delay│                                              │       cancelled     ││       cancelled     │                       │       cancelled     ││       cancelled     │
                       │ Stop:Departure delay││   = Stop:Departure  │                                              │ Stop:Departure delay││ Stop:Departure delay│                       │ Stop:Departure delay││ Stop:Departure delay│
                       │     Stop:Arrival    ││         delay       │                                              │     Stop:Arrival    ││     Stop:Arrival    │                       │     Stop:Arrival    ││     Stop:Arrival    │
                       │       cancelled     ││     Stop:Arrival    │                                              │       cancelled     ││       cancelled     │                       │       cancelled     ││       cancelled     │
                       │  Stop:Arrival time  ││   cancelled = Stop  │                                              │  Stop:Arrival time  ││  Stop:Arrival time  │                       │  Stop:Arrival time  ││  Stop:Arrival time  │
                       │ Service:Train number││  :Arrival cancelled │                                              │ Service:Train number││ Service:Train number│                       │ Service:Train number││ Service:Train number│
                       │     Stop:RDT-ID     ││ Stop:Arrival time = │                                              │     Stop:RDT-ID     ││     Stop:RDT-ID     │                       │     Stop:RDT-ID     ││     Stop:RDT-ID     │
                       │ Stop:Departure time ││   Stop:Arrival time │                                              │ Stop:Departure time ││ Stop:Departure time │                       │ Stop:Departure time ││ Stop:Departure time │
                       │Service:Maximum delay││ Service:Train number│                                              │Service:Maximum delay││Service:Maximum delay│                       │Service:Maximum delay││Service:Maximum delay│
                       │    Service:Partly   ││    = Service:Train  │                                              │    Service:Partly   ││    Service:Partly   │                       │    Service:Partly   ││    Service:Partly   │
                       │       cancelled     ││        number       │                                              │       cancelled     ││       cancelled     │                       │       cancelled     ││       cancelled     │
                       │  Service:Completely ││  Stop:RDT-ID = Stop │                                              │  Service:Completely ││  Service:Completely │                       │  Service:Completely ││  Service:Completely │
                       │       cancelled     ││       :RDT-ID       │                                              │       cancelled     ││       cancelled     │                       │       cancelled     ││       cancelled     │
                       │  Stop:Station code  ││ Stop:Departure time │                                              │  Stop:Station code  ││  Stop:Station code  │                       │  Stop:Station code  ││  Stop:Station code  │
                       │     Service:Date    ││= Stop:Departure time│                                              │     Service:Date    ││     Service:Date    │                       │     Service:Date    ││     Service:Date    │
                       │  Stop:Arrival delay ││   Service:Maximum   │                                              │  Stop:Arrival delay ││  Stop:Arrival delay │                       │  Stop:Arrival delay ││  Stop:Arrival delay │
                       │   Service:Company   ││    delay = Service  ├───────────┐                                  │   Service:Company   ││   Service:Company   │                       │   Service:Company   ││   Service:Company   │
                       │  Stop:Station name  ││    :Maximum delay   │           │                                  │  Stop:Station name  ││  Stop:Station name  │                       │  Stop:Station name  ││  Stop:Station name  │
                       │     Service:Type    ││    Service:Partly   │           │                                  │     Service:Type    ││     Service:Type    │                       │     Service:Type    ││     Service:Type    │
                       │    Service:RDT-ID   ││  cancelled = Service│           │                                  │    Service:RDT-ID   ││    Service:RDT-ID   │                       │    Service:RDT-ID   ││    Service:RDT-ID   │
                       │                     ││  :Partly cancelled  │           │                                  │                     ││                     │                       │                     ││                     │
                       │                     ││  Service:Completely │           │                                  │                     ││                     │                       │                     ││                     │
                       │                     ││  cancelled = Service│           │                                  │                     ││                     │                       │                     ││                     │
                       │                     ││:Completely cancelled│           │                                  │                     ││                     │                       │                     ││                     │
                       │                     ││ Stop:Station code = │           │                                  │                     ││                     │                       │                     ││                     │
                       │                     ││   Stop:Station code │           │                                  │                     ││                     │                       │                     ││                     │
                       │                     ││    Service:Date =   │           │                                  │                     ││                     │                       │                     ││                     │
                       │                     ││     Service:Date    │           │                                  │                     ││                     │                       │                     ││                     │
                       │                     ││ Stop:Arrival delay =│           │                                  │                     ││                     │                       │                     ││                     │
                       │                     ││  Stop:Arrival delay │           │                                  │                     ││                     │                       │                     ││                     │
                       │                     ││  Service:Company =  │           │                                  │                     ││                     │                       │                     ││                     │
                       │                     ││    Service:Company  │           │                                  │                     ││                     │                       │                     ││                     │
                       │                     ││ Stop:Station name = │           │                                  │                     ││                     │                       │                     ││                     │
                       │                     ││   Stop:Station name │           │                                  │                     ││                     │                       │                     ││                     │
                       │                     ││    Service:Type =   │           │                                  │                     ││                     │                       │                     ││                     │
                       │                     ││     Service:Type    │           │                                  │                     ││                     │                       │                     ││                     │
                       │                     ││   Service:RDT-ID =  │           │                                  │                     ││                     │                       │                     ││                     │
                       │                     ││    Service:RDT-ID   │           │                                  │                     ││                     │                       │                     ││                     │
                       │                     ││                     │           │                                  │                     ││                     │                       │                     ││                     │
                       │    8,684,407 rows   ││    8,684,407 rows   │           │                                  │    8,684,407 rows   ││   11,148,373 rows   │                       │    8,684,407 rows   ││   11,148,373 rows   │
                       │       (24.97s)      ││       (23.33s)      │           │                                  │       (14.50s)      ││       (3.57s)       │                       │       (14.00s)      ││       (7.99s)       │
                       └─────────────────────┘└──────────┬──────────┘           │                                  └─────────────────────┘└─────────────────────┘                       └─────────────────────┘└─────────────────────┘
                                              ┌──────────┴──────────┐┌──────────┴──────────┐
                                              │      TABLE_SCAN     ││      HASH_JOIN      │
                                              │    ──────────────   ││    ──────────────   │
                                              │     Table: tab8     ││      Join Type:     │
                                              │                     ││        INNER        │
                                              │        Type:        ││                     │
                                              │   Sequential Scan   ││     Conditions:     │
                                              │                     ││    Stop:Departure   │
                                              │     Projections:    ││   cancelled = Stop  │
                                              │    Stop:Departure   ││ :Departure cancelled│
                                              │       cancelled     ││ Stop:Departure delay│
                                              │ Stop:Departure delay││   = Stop:Departure  │
                                              │     Stop:Arrival    ││         delay       │
                                              │       cancelled     ││     Stop:Arrival    │
                                              │  Stop:Arrival time  ││   cancelled = Stop  │
                                              │ Service:Train number││  :Arrival cancelled │
                                              │     Stop:RDT-ID     ││ Stop:Arrival time = │
                                              │ Stop:Departure time ││   Stop:Arrival time │
                                              │Service:Maximum delay││ Service:Train number│
                                              │    Service:Partly   ││    = Service:Train  │
                                              │       cancelled     ││        number       │
                                              │  Service:Completely ││  Stop:RDT-ID = Stop │
                                              │       cancelled     ││       :RDT-ID       │
                                              │  Stop:Station code  ││ Stop:Departure time │
                                              │     Service:Date    ││= Stop:Departure time│
                                              │  Stop:Arrival delay ││   Service:Maximum   │
                                              │   Service:Company   ││    delay = Service  ├───────────┐
                                              │  Stop:Station name  ││    :Maximum delay   │           │
                                              │     Service:Type    ││    Service:Partly   │           │
                                              │    Service:RDT-ID   ││  cancelled = Service│           │
                                              │                     ││  :Partly cancelled  │           │
                                              │                     ││  Service:Completely │           │
                                              │                     ││  cancelled = Service│           │
                                              │                     ││:Completely cancelled│           │
                                              │                     ││ Stop:Station code = │           │
                                              │                     ││   Stop:Station code │           │
                                              │                     ││    Service:Date =   │           │
                                              │                     ││     Service:Date    │           │
                                              │                     ││ Stop:Arrival delay =│           │
                                              │                     ││  Stop:Arrival delay │           │
                                              │                     ││  Service:Company =  │           │
                                              │                     ││    Service:Company  │           │
                                              │                     ││ Stop:Station name = │           │
                                              │                     ││   Stop:Station name │           │
                                              │                     ││    Service:Type =   │           │
                                              │                     ││     Service:Type    │           │
                                              │                     ││   Service:RDT-ID =  │           │
                                              │                     ││    Service:RDT-ID   │           │
                                              │                     ││                     │           │
                                              │    8,684,407 rows   ││    8,684,407 rows   │           │
                                              │       (12.88s)      ││       (13.85s)      │           │
                                              └─────────────────────┘└──────────┬──────────┘           │
                                                                     ┌──────────┴──────────┐┌──────────┴──────────┐
                                                                     │      TABLE_SCAN     ││      TABLE_SCAN     │
                                                                     │    ──────────────   ││    ──────────────   │
                                                                     │     Table: tab9     ││     Table: tab10    │
                                                                     │                     ││                     │
                                                                     │        Type:        ││        Type:        │
                                                                     │   Sequential Scan   ││   Sequential Scan   │
                                                                     │                     ││                     │
                                                                     │     Projections:    ││     Projections:    │
                                                                     │    Stop:Departure   ││    Stop:Departure   │
                                                                     │       cancelled     ││       cancelled     │
                                                                     │ Stop:Departure delay││ Stop:Departure delay│
                                                                     │     Stop:Arrival    ││     Stop:Arrival    │
                                                                     │       cancelled     ││       cancelled     │
                                                                     │  Stop:Arrival time  ││  Stop:Arrival time  │
                                                                     │ Service:Train number││ Service:Train number│
                                                                     │     Stop:RDT-ID     ││     Stop:RDT-ID     │
                                                                     │ Stop:Departure time ││ Stop:Departure time │
                                                                     │Service:Maximum delay││Service:Maximum delay│
                                                                     │    Service:Partly   ││    Service:Partly   │
                                                                     │       cancelled     ││       cancelled     │
                                                                     │  Service:Completely ││  Service:Completely │
                                                                     │       cancelled     ││       cancelled     │
                                                                     │  Stop:Station code  ││  Stop:Station code  │
                                                                     │     Service:Date    ││     Service:Date    │
                                                                     │  Stop:Arrival delay ││  Stop:Arrival delay │
                                                                     │   Service:Company   ││   Service:Company   │
                                                                     │  Stop:Station name  ││  Stop:Station name  │
                                                                     │     Service:Type    ││     Service:Type    │
                                                                     │    Service:RDT-ID   ││    Service:RDT-ID   │
                                                                     │                     ││                     │
                                                                     │    8,684,407 rows   ││   11,148,373 rows   │
                                                                     │       (14.30s)      ││       (9.74s)       │
                                                                     └─────────────────────┘└─────────────────────┘
D 

