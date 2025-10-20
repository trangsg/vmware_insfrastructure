------------------------------------------------------------------------------------
-- QUERY TITLE: Dynamic Pivot for vHBA Information by Host (Fictional)
-- DESCRIPTION:
-- This script dynamically pivots the vHBA data (Device, WorldwideName, Status)
-- from the Fictional_VMware_HBA table so that each host (HostName)
-- displays its vHBA details as separate numbered columns:
--     vHBADevice_1, vHBAWorldwideName_1, vHBAStatus_1,
--     vHBADevice_2, vHBAWorldwideName_2, vHBAStatus_2, etc.
-- The dynamic pivot ensures that only non-empty vHBA values are included.
------------------------------------------------------------------------------------

-- Declare a variable to hold the dynamic SQL query
DECLARE @SQL NVARCHAR(MAX);

------------------------------------------------------------------------------------
-- STEP 1: Generate dynamic column names for non-empty vHBA entries
------------------------------------------------------------------------------------
SET @SQL = STUFF(
    (SELECT DISTINCT
         ',' + QUOTENAME('vHBADevice_' + CAST(RN AS NVARCHAR)) +
         ',' + QUOTENAME('vHBAWorldwideName_' + CAST(RN AS NVARCHAR)) +
         ',' + QUOTENAME('vHBAStatus_' + CAST(RN AS NVARCHAR))
     FROM (
         SELECT
             HostName,
             ROW_NUMBER() OVER (PARTITION BY HostName ORDER BY Device) AS RN,
             Device,
             WorldwideName,
             Status
         FROM Fictional_VMware_HBA
         WHERE Device IS NOT NULL 
            OR WorldwideName IS NOT NULL 
            OR Status IS NOT NULL
     ) AS Source
     FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '');

------------------------------------------------------------------------------------
-- STEP 2: Build the dynamic SQL query string for the pivot
------------------------------------------------------------------------------------
SET @SQL = '
SELECT HostName, ' + @SQL + '
FROM (
    SELECT
        HostName,
        ''vHBADevice_'' + CAST(RN AS NVARCHAR) AS Col,
        Device AS Value
    FROM (
        SELECT
            HostName,
            Device,
            WorldwideName,
            Status,
            ROW_NUMBER() OVER (PARTITION BY HostName ORDER BY Device) AS RN
        FROM Fictional_VMware_HBA
        WHERE Device IS NOT NULL 
           OR WorldwideName IS NOT NULL 
           OR Status IS NOT NULL
    ) A
    CROSS APPLY (
        VALUES
        (''vHBADevice_'' + CAST(A.RN AS NVARCHAR), A.Device),
        (''vHBAWorldwideName_'' + CAST(A.RN AS NVARCHAR), A.WorldwideName),
        (''vHBAStatus_'' + CAST(A.RN AS NVARCHAR), A.Status)
    ) B (Col, Value)
) P
PIVOT (
    MAX(Value)
    FOR Col IN (' + @SQL + ')
) AS PivotedTable;';

------------------------------------------------------------------------------------
-- STEP 3: Execute the dynamically generated SQL
------------------------------------------------------------------------------------
EXEC sp_executesql @SQL;
