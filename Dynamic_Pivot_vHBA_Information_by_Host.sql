------------------------------------------------------------------------------------
-- QUERY TITLE: Dynamic Pivot for vHBA Information by Host
-- DESCRIPTION:
-- This script dynamically pivots the vHBA data (Device, WorldwideName, Status)
-- from the VMWARE_RVTOOLS_tabvHBA table so that each host (vHBAHostName)
-- displays its vHBA details as separate numbered columns:
--     vHBADevice_1, vHBAWorldwideName_1, vHBAStatus_1,
--     vHBADevice_2, vHBAWorldwideName_2, vHBAStatus_2, etc.
-- The dynamic pivot ensures that only non-empty vHBA values are included.
------------------------------------------------------------------------------------

-- Declare a variable to hold the dynamic SQL query
DECLARE @SQL NVARCHAR(MAX);

------------------------------------------------------------------------------------
-- STEP 1: Generate dynamic column names for non-empty vHBA entries
-- The STUFF() + FOR XML PATH('') technique concatenates the column list
-- (e.g., vHBADevice_1, vHBAWorldwideName_1, vHBAStatus_1, ...)
-- based on the number of vHBAs found for each host.
------------------------------------------------------------------------------------
SET @SQL = STUFF(
    (SELECT DISTINCT
         ',' + QUOTENAME('vHBADevice_' + CAST(RN AS NVARCHAR)) +
         ',' + QUOTENAME('vHBAWorldwideName_' + CAST(RN AS NVARCHAR)) +
         ',' + QUOTENAME('vHBAStatus_' + CAST(RN AS NVARCHAR))
     FROM (
         SELECT
             vHBAHostName,
             ROW_NUMBER() OVER (PARTITION BY vHBAHostName ORDER BY vHBADevice) AS RN,
             vHBADevice,
             vHBAWorldwideName,
             vHBAStatus
         FROM [VMWARE_RVTOOLS].[dbo].[VMWARE_RVTOOLS_tabvHBA]
         WHERE vHBADevice IS NOT NULL 
            OR vHBAWorldwideName IS NOT NULL 
            OR vHBAStatus IS NOT NULL
     ) AS Source
     FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '');

------------------------------------------------------------------------------------
-- STEP 2: Build the dynamic SQL query string for the pivot
-- This section prepares a pivot table that transforms rows into columns dynamically.
-- Each host’s vHBA entries (device, WWN, status) are expanded across columns.
------------------------------------------------------------------------------------
SET @SQL = '
SELECT vHBAHostName, ' + @SQL + '
FROM (
    SELECT
        vHBAHostName,
        ''vHBADevice_'' + CAST(RN AS NVARCHAR) AS Col,
        vHBADevice AS Value
    FROM (
        SELECT
            vHBAHostName,
            vHBADevice,
            vHBAWorldwideName,
            vHBAStatus,
            ROW_NUMBER() OVER (PARTITION BY vHBAHostName ORDER BY vHBADevice) AS RN
        FROM [VMWARE_RVTOOLS].[dbo].[VMWARE_RVTOOLS_tabvHBA]
        WHERE vHBADevice IS NOT NULL 
           OR vHBAWorldwideName IS NOT NULL 
           OR vHBAStatus IS NOT NULL
    ) A
    CROSS APPLY (
        VALUES
        (''vHBADevice_'' + CAST(A.RN AS NVARCHAR), A.vHBADevice),
        (''vHBAWorldwideName_'' + CAST(A.RN AS NVARCHAR), A.vHBAWorldwideName),
        (''vHBAStatus_'' + CAST(A.RN AS NVARCHAR), A.vHBAStatus)
    ) B (Col, Value)
) P
PIVOT (
    MAX(Value)
    FOR Col IN (' + @SQL + ')
) AS PivotedTable;';

------------------------------------------------------------------------------------
-- STEP 3: Execute the dynamically generated SQL
-- This produces a pivoted result set where each host has its vHBA
-- devices, worldwide names, and statuses displayed in numbered columns.
------------------------------------------------------------------------------------
EXEC sp_executesql @SQL;
