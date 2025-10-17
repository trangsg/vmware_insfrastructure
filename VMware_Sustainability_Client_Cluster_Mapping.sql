CREATE VIEW VMware_Sustainability_Client_Cluster_Mapping AS
-- Define a Common Table Expression (CTE) to extract hardware information
WITH hardware AS (
    SELECT
        hw.name AS name,                          -- Hardware name
        hw.company AS company,                    -- Company sys_id reference from cmdb_ci_pc_hardware
        hw.dv_u_environment,                      -- Environment of the hardware
        company.name AS company_name,             -- Company name from core_company
        company.dv_u_cluster,                     -- Cluster associated with the company
        company.u_company_code                    -- Company code
    FROM mirror.dbo.cmdb_ci_pc_hardware AS hw
    LEFT JOIN mirror.dbo.core_company AS company  -- Join to get company details
        ON hw.company = company.sys_id
),

-- Define a CTE to extract server information with environment-based company mapping logic
server AS (
    SELECT
        srv.name,                                 -- Server name
        srv.company AS company_id,                -- Server company sys_id
        srv.dv_u_environment,                     -- Environment of the server
        srv.company,                              -- Original company sys_id (duplicated for reference)
        dv_company,                               -- Display value of company
        srv.dv_u_user_company,                    -- User company display value
        -- Determine the correct company name to use based on environment and company conditions
        CASE 
            WHEN srv.dv_u_environment = 'CA-TS' 
                 AND srv.company = 'be6bc25c1b645090a17d62ce6e4bcb16'
            THEN srv.dv_u_user_company
            ELSE dv_company 
        END AS srv_company,
        company.name AS company_name,             -- Company name
        company.dv_u_cluster,                     -- Cluster associated with the company
        company.sys_id,                           -- Company sys_id
        company.u_company_code                    -- Company code
    FROM mirror.dbo.cmdb_ci_server AS srv
    LEFT JOIN mirror.dbo.core_company AS company  -- Join to get company details
        ON company.name = (
            CASE 
                WHEN srv.dv_u_environment = 'CA-TS' 
                     AND srv.company = 'be6bc25c1b645090a17d62ce6e4bcb16'
                THEN srv.dv_u_user_company
                ELSE dv_company 
            END
        )
)

-- Final query to merge VMware sustainability data with hardware and server information
SELECT
    s.*,                                          -- Select all fields from VMWARE_Sustainability
    -- Determine the final client name, preferring whichever source (hardware/server) has a value
    CASE 
        WHEN hardware.company_name IS NULL AND server.company_name IS NOT NULL THEN server.company_name
        WHEN hardware.company_name IS NOT NULL AND server.company_name IS NULL THEN hardware.company_name
        ELSE server.company_name 
    END AS Client,

    -- Determine the final cluster, prioritizing whichever source provides it
    CASE 
        WHEN hardware.dv_u_cluster IS NULL AND server.dv_u_cluster IS NOT NULL THEN server.dv_u_cluster
        WHEN hardware.dv_u_cluster IS NOT NULL AND server.dv_u_cluster IS NULL THEN hardware.dv_u_cluster
        ELSE server.dv_u_cluster 
    END AS Cluster

FROM VMWARE.dbo.VMWARE_Sustainability AS s         -- VMware sustainability base data
LEFT JOIN hardware
    ON s.Nom = hardware.name                       -- Match sustainability record to hardware by name
LEFT JOIN server
    ON s.Nom = server.name                         -- Match sustainability record to server by name
