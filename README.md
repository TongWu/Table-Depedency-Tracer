# Table Dependency Tracer

This is a buttom-up table dependency tracer prototype.

## Achieved Function

- Find table dependency based on input string of table name (with/without database name), the dependency is strict by database_name.table_name
- The compability includes python and sql scripts
- Generate script-to-target mapping CSVs via `generate_script_target_mapping.py`

## TODO list

- Support SAS and datastage script
- Support generic matching for target table in a script (use insertinto(variable_name) to find the target table name)
- Modulise the code, improve readiness

## Script to Target Mapping

Run `generate_script_target_mapping.py` to build a CSV containing two columns (`script name`, `target table`).

```bash
python generate_script_target_mapping.py --root <code_folder> --out script_target_mapping.csv
```

The script scans Python files (AST plus header parsing) and SQL files (view definitions) under the given root. Scripts that write to multiple targets are emitted as multiple rows in the CSV.
