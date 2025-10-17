/***************************************************************
  Title:     VMware Cluster Resource Summary by Project and Availability Type
  Purpose:   Aggregate VM resource data by project and site redundancy.

  Description:
      - Parses the project reference (u_project_reference) to extract 
        a "lios" project identifier.
      - Classifies clusters as Monosite (single-site) or Bisite (dual-site)
        based on the u_high_availability flag.
      - Calculates:
            * Total CPU cores per project & site type
            * Total memory (in GB) per project & site type
            * Weighted capacity score (uo_puissance)
      - Filters out decommissioned clusters (dv_install_status != 'Retiré')
***************************************************************/

WITH ProjectResourceSummary AS (
    SELECT DISTINCT
        -----------------------------------------------------------------
        -- Extract 'lios' project identifier from u_project_reference
        -- Example format: PROJ-XXX-YYY-LIOS-ZZZ  → Extract 'LIOS-ZZZ'
        -----------------------------------------------------------------
        CONCAT(
            SUBSTRING(
                u_project_reference,
                CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference) + 1) + 1) + 1,
                CHARINDEX('-', u_project_reference,
                    CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference) + 1) + 1) + 1
                )
                - CHARINDEX('-', u_project_reference,
                    CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference) + 1) + 1
                ) - 1
            ),
            '-',
            RIGHT(
                u_project_reference,
                LEN(u_project_reference) - CHARINDEX('-',
                    u_project_reference,
                    CHARINDEX('-',
                        u_project_reference,
                        CHARINDEX('-',
                            u_project_reference,
                            CHARINDEX('-', u_project_reference) + 1
                        ) + 1
                    ) + 1
                )
            )
        ) AS lios,

        -----------------------------------------------------------------
        -- Determine site redundancy type: Monosite or Bisite
        -----------------------------------------------------------------
        CASE
            WHEN u_high_availability = '0' THEN 'Monosite'
            WHEN u_high_availability = '1' THEN 'Bisite'
            ELSE NULL
        END AS [Mono/Bisite],

        -----------------------------------------------------------------
        -- Total CPU cores per project & redundancy type
        -----------------------------------------------------------------
        SUM(CAST(vInfoCPUs AS INT)) OVER (
            PARTITION BY CONCAT(
                SUBSTRING(u_project_reference,
                    CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference) + 1) + 1) + 1,
                    CHARINDEX('-', u_project_reference,
                        CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference) + 1) + 1) + 1
                    )
                    - CHARINDEX('-', u_project_reference,
                        CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference) + 1) + 1
                    ) - 1
                ),
                '-',
                RIGHT(
                    u_project_reference,
                    LEN(u_project_reference) - CHARINDEX('-',
                        u_project_reference,
                        CHARINDEX('-',
                            u_project_reference,
                            CHARINDEX('-',
                                u_project_reference,
                                CHARINDEX('-', u_project_reference) + 1
                            ) + 1
                        ) + 1
                    )
                ),
                '-',
                CASE
                    WHEN u_high_availability = '0' THEN 'Monosite'
                    WHEN u_high_availability = '1' THEN 'Bisite'
                    ELSE NULL
                END
            )
        ) AS Total_CPU,

        -----------------------------------------------------------------
        -- Total memory (GB) per project & redundancy type
        -----------------------------------------------------------------
        SUM(CAST(vInfoMemory_Go AS FLOAT)) OVER (
            PARTITION BY CONCAT(
                SUBSTRING(u_project_reference,
                    CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference) + 1) + 1) + 1,
                    CHARINDEX('-', u_project_reference,
                        CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference) + 1) + 1) + 1
                    )
                    - CHARINDEX('-', u_project_reference,
                        CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference) + 1) + 1
                    ) - 1
                ),
                '-',
                RIGHT(
                    u_project_reference,
                    LEN(u_project_reference) - CHARINDEX('-',
                        u_project_reference,
                        CHARINDEX('-',
                            u_project_reference,
                            CHARINDEX('-',
                                u_project_reference,
                                CHARINDEX('-', u_project_reference) + 1
                            ) + 1
                        ) + 1
                    )
                ),
                '-',
                CASE
                    WHEN u_high_availability = '0' THEN 'Monosite'
                    WHEN u_high_availability = '1' THEN 'Bisite'
                    ELSE NULL
                END
            )
        ) AS Total_MEM,

        -----------------------------------------------------------------
        -- Weighted capacity metric ("uo_puissance")
        -- Formula: (4 * CPU + Memory) * coefficient (based on cluster)
        -----------------------------------------------------------------
        SUM(
            (4 * CAST(vInfoCPUs AS INT) + CAST(vInfoMemory_Go AS FLOAT)) * coef_applique
        ) OVER (
            PARTITION BY CONCAT(
                SUBSTRING(u_project_reference,
                    CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference) + 1) + 1) + 1,
                    CHARINDEX('-', u_project_reference,
                        CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference) + 1) + 1) + 1
                    )
                    - CHARINDEX('-', u_project_reference,
                        CHARINDEX('-', u_project_reference, CHARINDEX('-', u_project_reference) + 1) + 1
                    ) - 1
                ),
                '-',
                RIGHT(
                    u_project_reference,
                    LEN(u_project_reference) - CHARINDEX('-',
                        u_project_reference,
                        CHARINDEX('-',
                            u_project_reference,
                            CHARINDEX('-',
                                u_project_reference,
                                CHARINDEX('-', u_project_reference) + 1
                            ) + 1
                        ) + 1
                    )
                ),
                '-',
                CASE
                    WHEN u_high_availability = '0' THEN 'Monosite'
                    WHEN u_high_availability = '1' THEN 'Bisite'
                    ELSE NULL
                END
            )
        ) AS uo_puissance

    -----------------------------------------------------------------
    -- Source tables and joins
    -----------------------------------------------------------------
    FROM [VMWARE_RVTOOLS].[dbo].[VMWARE_RVTools_tabvCluster] AS cluster
    LEFT JOIN [VMWARE].[dbo].[VMWARE_RVTools_tabvInfo_VMWARE] AS vinfo
        ON vClusterName = vInfoCluster
    LEFT JOIN (
        SELECT
            name,
            dv_install_status,
            u_high_availability,
            u_project_reference,
            dv_u_ci_criticality,
            MAX(u_billing_function) AS u_billing_function
        FROM [mirror].[dbo].[cmdb_ci_cluster]
        WHERE u_billing_function <> ''
        GROUP BY
            name,
            dv_install_status,
            u_high_availability,
            u_project_reference,
            dv_u_ci_criticality
    ) AS filtered_cluster
        ON vClusterName = filtered_cluster.name

    -----------------------------------------------------------------
    -- Exclude decommissioned clusters
    -----------------------------------------------------------------
    WHERE dv_install_status != 'Retiré'
)

-----------------------------------------------------------------
-- Final output: aggregated resource metrics by project and site type
-----------------------------------------------------------------
SELECT *
FROM ProjectResourceSummary;
