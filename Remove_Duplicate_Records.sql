/*This SQL query identifies and removes duplicate service records based on the combination of server, application, and service name.
It uses a window function (ROW_NUMBER()) to assign a sequential number to each record within these groups and retains only the first occurrence of each unique combination.
The goal is to produce a clean dataset of unique service entries with their related metadata (e.g., relationships, criticality, and classification).*/

-- ============================================
-- QUERY: Remove Duplicate Service Records (Fictional)
-- ============================================
-- Purpose:
--   - Identify and keep only one unique record per (server, app, service_name)
--   - Use ROW_NUMBER() to rank duplicates and select the first occurrence
-- ============================================

WITH ranked_services AS (
    SELECT
        -- Basic service identifiers
        srv.ServerName,                     -- Name or ID of the server hosting the service
        srv.AppName,                        -- Associated application name or ID
        srv.ServiceName,                    -- Name of the specific service

        -- Service metadata and relationships
        srv.ServiceID,                      -- Unique ID of the service
        srv.ChildServiceRef,                -- Reference to child service (dependency)
        srv.ParentServiceRef,               -- Reference to parent service

        -- Application metadata
        srv.AppID,                           -- Unique ID of the application
        srv.AppClassName,                    -- Application class
        srv.AppSubtype,                      -- Application subtype
        srv.ServiceCriticality,              -- Service criticality level

        -- Parent service offering
        srv.ParentOfferingID,                -- Parent service offering reference
        srv.ServiceSysID,                    -- Service system ID

        -- Assign a ranking within each group of (server, app, service_name)
        ROW_NUMBER() OVER (
            PARTITION BY srv.ServerName, srv.AppName, srv.ServiceName
            ORDER BY srv.ServiceSysID
        ) AS row_num

    FROM
        Fictional_ServiceTable AS srv
    LEFT JOIN 
        Fictional_ServiceOffering AS offering
        ON srv.ParentOfferingID = offering.OfferingID   -- Join to get parent offering details
)

-- ============================================
-- Final selection:
-- Retrieve only the first record (row_num = 1)
-- to eliminate duplicates
-- ============================================

SELECT
    ServerName,
    AppName,
    ServiceName,
    ServiceID,
    ChildServiceRef,
    ParentServiceRef,
    AppID,
    AppClassName,
    AppSubtype,
    ServiceCriticality,
    ParentOfferingID,
    ServiceSysID
FROM
    ranked_services
WHERE
    row_num = 1;   -- Keep only the first record in each group
