CREATE VIEW vw_Sustainability_Client_Cluster AS
-- Define a Common Table Expression (CTE) to extract hardware information
WITH hardware AS (
    SELECT
        hw.name AS name,                          -- Hardware name
        hw.company AS company,                    -- Company ID reference from hardware table
        hw.environment AS environment,            -- Environment of the hardware
        comp.name AS company_name,                -- Company name
        comp.cluster AS cluster,                  -- Cluster associated with the company
        comp.company_code AS company_code         -- Company code
    FROM fictional_db.hardware_inventory AS hw
    LEFT JOIN fictional_db.company_master AS comp   -- Join to get company details
        ON hw.company = comp.sys_id
),

-- Define a CTE to extract server information with environment-based company mapping logic
server AS (
    SELECT
        srv.name,                                 -- Server name
        srv.company AS company_id,                -- Server company ID
        srv.environment AS environment,           -- Environment of the server
        srv.company,                              -- Original company ID
        srv.display_company AS display_company,   -- Display value of company
        srv.user_company AS user_company,         -- User-defined company display value
        -- Determine the correct company name based on environment and company conditions
        CASE 
            WHEN srv.environment = '<TARGET_ENVIRONMENT>' 
                 AND srv.company = '<TARGET_COMPANY_ID>'
            THEN srv.user_company
            ELSE srv.display_company 
        END AS srv_company,
        comp.name AS company_name,                -- Company name
        comp.cluster AS cluster,                  -- Cluster associated with the company
        comp.sys_id AS company_sys_id,            -- Company ID
        comp.company_code AS company_code         -- Company code
    FROM fictional_db.server_inventory AS srv
    LEFT JOIN fictional_db.company_master AS comp   -- Join to get company details
        ON comp.name = (
            CASE 
                WHEN srv.environment = '<TARGET_ENVIRONMENT>' 
                     AND srv.company = '<TARGET_COMPANY_ID>'
                THEN srv.user_company
                ELSE srv.display_company 
            END
        )
)

-- Final query to merge sustainability data with hardware and server information
SELECT
    s.*,                                          -- Select all fields from sustainability data
    -- Determine the final client name
    CASE 
        WHEN hardware.company_name IS NULL AND server.company_name IS NOT NULL THEN server.company_name
        WHEN hardware.company_name IS NOT NULL AND server.company_name IS NULL THEN hardware.company_name
        ELSE server.company_name 
    END AS Client,

    -- Determine the final cluster
    CASE 
        WHEN hardware.cluster IS NULL AND server.cluster IS NOT NULL THEN server.cluster
        WHEN hardware.cluster IS NOT NULL AND server.cluster IS NULL THEN hardware.cluster
        ELSE server.cluster 
    END AS Cluster

FROM fictional_db.sustainability_data AS s        -- Base sustainability data
LEFT JOIN hardware
    ON s.resource_name = hardware.name            -- Match by hardware name
LEFT JOIN server
    ON s.resource_name = server.name              -- Match by server name
