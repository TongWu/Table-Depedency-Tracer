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

## Layer-to-target expander

`LayerToTargetExpander.py` post-processes the lineage CSV produced by
`TableDependencyTracer.py`. It scans each row and promotes layer tables into
the "Target Table" column (while keeping the downstream dependency chain)
when those tables do not already appear as top-level targets.

```
python LayerToTargetExpander.py --input lineage.csv --output lineage_expanded.csv
```

The command above reads `lineage.csv`, appends new rows for each eligible layer
table and writes the expanded result to `lineage_expanded.csv`.
