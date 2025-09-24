# Table Dependency Tracer

This is a buttom-up table dependency tracer prototype.

## Achieved Function

- Find table dependency based on input string of table name (with/without database name), the dependency is strict by database_name.table_name
- The compability includes python and sql scripts

## Utilities

- `ExpandLayerDependencies.py`: promote intermediate `Layer N` tables produced by
  `TableDependencyTracer.py` into standalone target rows. This is useful when you
  want dependency chains for every layer without manually seeding them as
  targets. Run it on an existing lineage CSV:

  ```bash
  python ExpandLayerDependencies.py --input lineage.csv --output expanded.csv
  ```

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
