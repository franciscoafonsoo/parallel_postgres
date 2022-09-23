# Parellel Loader - Faster Postgres to Postgres Migrations
pg\_dump/pg\_restore can sometimes might not optimal approach for data migration because:
- pg\_restore has to wait until pg\_dump finishes. Can be bottlenecked on client IO.
- pg\_dump/pg\_restore cannot run in parallel across a single table. You can parallelize them across tables but a single table can be read/written only via a single threads. This can be done if the table is partitioned though.

This tool address both the above issues:
- Simultaneous reading and writing from source table
- Ability to use multi-threading to dump/restore on a single table

The tool chunks out the source table based on a watermark (id) columns and uses multiple threads to read from the source table and write into destination table. It uses COPY command in postgres for the reading and writing.

## Benefits
- Migrate to larger disks/storage (>4TB) in PaaS environment.
- Faster offline upgrades 
- Faster offline migrations. 
- If workload is append only, help improve initial load and reduce downtime for the lag (during initial load) to be applied. 

## Usage:

1. Modify the `Config` class to include the connection strings necessary

2. Execute the command using `python` or `pypy`

```
pypy parallel_migrate.py -s wus -st erange_private.raw_signals -d local -dt erange_private.raw_signals -c 1097557 -d 2
```

*count_of_table* can be got by query the sequence value:
Ex:
```
select count(*) from table where device_id in (...);
```

Currently the usage is restricted to migrate a single large table. But you can run multiple instances of this script to migrate multiple tables. Customers can compliment this script with pg_dump/pg_restore ie. use pg_dump/pg_restore for the smaller tables and parallel loader for large tables.

## Advices

This was tested using `pypy3.9-7.3.9` for extra performance.

## Pre-requisites/Recommendations
- sequential id column has to be present on the table.
- Index on that id column is strongly recommended because the script splits the table based on the id column. Under the covers it uses WHERE clause filters on start and end ranges of the id column based on the number of threads you specify. It splits the total count into number\_of\_threads chunks.
- Also it is recommended that the source db, client (can be azure VM) and the destination db are colocated in the same region. Switch on accelrated networking on the client.
- As data-migration migrations are not permanent, recommendation is to beefup the source db, destination db and client vm hardware.
- While using parallel loader, don't forget to reset the sequences on the table on the destination database once data is migrated. It doesn't handle resetting sequences on the table you are migrating.

## Sample results
-  Migrated 1.4TB table from Azure Database for PostgreSQL Single-Server to another Single-Server in *7 hours and 45 minutes*. Medium sized servers (16 vcores)
-  Can tweak this and make it migrate at a faster rate too - better network, more Vcores, more memory etc.
- ![Network Throughput: 10GB every 5 minutes](https://github.com/microsoft/OrcasNinjaTeam/blob/master/azure-postgresql/data_migration/image003.png) 
