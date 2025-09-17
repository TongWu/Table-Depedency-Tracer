# Table Dependency Tracer

This is a buttom-up table dependency tracer prototype.

## Achieved Function

- Find table dependency based on input string of table name (with/without database name), the dependency is strict by database_name.table_name
- The compability includes python and sql scripts

## TODO list

- Support SAS and datastage script
- Generate script name <-> table name mapping
- Support generic matching for target table in a script (use insertinto(variable_name) to find the target table name)
- Modulise the code, improve readiness
