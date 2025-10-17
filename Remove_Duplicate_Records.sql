
/*This SQL query identifies and removes duplicate service records based on the combination of server, application, and service name.
It uses a window function (ROW_NUMBER()) to assign a sequential number to each record within these groups and retains only the first occurrence of each unique combination.
The goal is to produce a clean dataset of unique service entries with their related metadata (e.g., relationships, criticality, and classification).*/
-- ============================================
-- QUERY: Remove Duplicate Service Records
-- ============================================
-- Purpose:
--   - Identify and keep only one unique record per (server, app, service_name)
--   - Use ROW_NUMBER() to rank duplicates and select the first occurrence
-- ============================================

WITH ranked_data AS (
    SELECT
        -- Basic service identifiers
        service.server,                   -- Name or ID of the server hosting the service
        service.app,                      -- Associated application name or ID
        service.service_name,             -- Name of the specific service

        -- Service system fields and relationships
        service.srv_sys_id,               -- System ID of the service
        service.rel_srv_child,            -- Relationship: child service (dependency)
        service.rel_srv_parent,           -- Relationship: parent service

        -- Application details
        service.app_sys_id,               -- System ID of the application
        service.app_dv_sys_class_name,    -- Application class name
        service.app_dv_u_subtype,         -- Application subtype (custom field)
        service.app_u_service_criticality,-- Service criticality level

        -- Parent relationship and service ID
        service.parent,                   -- Parent service offering reference
        service.service_sys_id,           -- Service record system ID

        -- Assign a ranking within each group of (server, app, service_name)
        ROW_NUMBER() OVER (
            PARTITION BY service.server, service.app, service.service_name
            ORDER BY service.service_sys_id
        ) AS row_num

    FROM
        service
    LEFT JOIN 
        service_offering 
        ON service.parent = service_offering.sys_id   -- Join to get parent offering details
)

-- ============================================
-- Final selection:
-- Retrieve only the first record (row_num = 1)
-- to eliminate duplicates
-- ============================================

SELECT
    server,
    app,
    service_name,
    srv_sys_id,
    rel_srv_child,
    rel_srv_parent,
    app_sys_id,
    app_dv_sys_class_name,
    app_dv_u_subtype,
    app_u_service_criticality,
    parent,
    service_sys_id
FROM
    ranked_data
WHERE
    row_num = 1;   -- Keep only the first record in each group
