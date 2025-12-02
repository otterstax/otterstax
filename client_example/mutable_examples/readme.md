Running examples sequentially:

1. Creates a new table with (temp_int int) column (TODO: expand column types)
2. Inserts one value
3. Inserts multiple values
4. Updates existing values
5. Delete some values
6. Inserts values after select from the same table
7. Creates index
8. Drops index
9. Drops table

Known issues:

1. Creating and dropping database will cause problems with connections
2. Creating table might not grant required privileges to modify it
3. Some SQL syntax is not complete and might be lost/broken in parsing-generation process