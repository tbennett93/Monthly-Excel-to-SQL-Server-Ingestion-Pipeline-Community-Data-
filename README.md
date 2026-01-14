# Monthly Excel to SQL Server Ingestion Pipeline Community Data
This project implements a monthly batch ingestion pipeline that loads community dashboard data from an Excel file into SQL Server, applying strict validation before persisting data to staging tables and executing downstream stored procedures.


## What the pipeline does

1. Calculates the previous month (including year rollover).

2. Reads an Excel file from a shared network location.

3. Validates data:
   - Enforces date format (`DD/MM/YYYY`)
   - Confirms data exists for the expected reporting period
   - Rejects unmapped values using reference sheets in the same workbook
   - Enforces column data types

4. Loads to SQL Server
   - Truncates staging tables
   - Bulk inserts validated data
   - Executes stored procedures for downstream processing
