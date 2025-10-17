-- ============================================
-- QUERY: Cluster-Level Summary of VMware Resources
-- ============================================
-- Purpose: Aggregate CPU, Host, and VM information by cluster,
--          along with extracted hardware identifiers from host details.
-- ============================================

-- --------------------------------------------
-- Subquery 1: HostAggregates
-- Calculates total CPUs, number of hosts, and
-- extracts manufacturer/model details per cluster.
-- --------------------------------------------

WITH HostAggregates AS (
    SELECT 
        vHostCluster,  -- Cluster name each host belongs to

        -- Total number of CPUs across all hosts in the cluster
        SUM(CAST(vHostNumCPU AS int)) AS TotalCPUs,

        -- Count of unique host machines in the cluster
        COUNT(DISTINCT vHostName) AS Nb_Hosts,

        -- Extract first word from manufacturer (e.g., "Dell" from "Dell Inc.")
        -- Skip if manufacturer name starts with "vmware"
        CASE
            WHEN LOWER(vHostManufacturer) LIKE 'vmware%' THEN NULL
            ELSE LEFT(vHostManufacturer, CHARINDEX(' ', vHostManufacturer + ' ') - 1)
        END AS FirstWord,

        -- Extract last two segments from model name (after the last '-')
        -- Skip if model name starts with "vmware"
        CASE
            WHEN LOWER(vHostModel) LIKE 'vmware%' THEN NULL
            ELSE REVERSE(LEFT(REVERSE(vHostModel), CHARINDEX('-', REVERSE(vHostModel) + '-') - 1))
        END AS LastTwoSegments,

        -- CPU information for each host
        vHostCpuNum AS CpuNum,
        vHostCpuCore AS CpuCore,

        -- Concatenate extracted manufacturer, model, and CPU details
        -- to form a unique hardware signature
        CONCAT(
            CASE
                WHEN LOWER(vHostManufacturer) LIKE 'vmware%' THEN ''
                ELSE LEFT(vHostManufacturer, CHARINDEX(' ', vHostManufacturer + ' ') - 1)
            END,
            '-',
            CASE
                WHEN LOWER(vHostModel) LIKE 'vmware%' THEN ''
                ELSE REVERSE(LEFT(REVERSE(vHostModel), CHARINDEX('-', REVERSE(vHostModel) + '-') - 1))
            END,
            '-',
            vHostCpuNum,
            '-',
            vHostCpuCore
        ) AS Result

    FROM 
        [VMWARE_RVTOOLS].[dbo].[VMWARE_RVTools_tabvHost]
),

-- --------------------------------------------
-- Subquery 2: VMAggregates
-- Calculates the number of VMs per cluster and
-- breaks them down by operating system.
-- --------------------------------------------

VMAggregates AS (
    SELECT 
        vInfoCluster,  -- Cluster name each VM belongs to

        -- Total number of distinct VMs per cluster
        COUNT(DISTINCT vInfoVMName) AS Nb_VM,

        -- Total count of OS entries (may include duplicates)
        COUNT(vInfoOS) AS vInfoOS_Total,

        -- Count of unique Windows VMs
        COUNT(DISTINCT CASE 
                           WHEN vInfoOS LIKE '%Windows%' THEN vInfoVMName
                      END) AS vInfoOS_Windows,

        -- Count of unique Linux VMs (specifically Red Hat)
        COUNT(DISTINCT CASE 
                           WHEN vInfoOS LIKE '%Red Hat%' THEN vInfoVMName
                      END) AS vInfoOS_Linux,

        -- Count of other OS types (not Windows or Red Hat)
        COUNT(DISTINCT CASE 
                           WHEN vInfoOS NOT LIKE '%Red Hat%' AND vInfoOS NOT LIKE '%Windows%' THEN vInfoVMName
                      END) AS vInfoOS_Others

    FROM 
        [VMWARE_RVTOOLS].[dbo].[VMWARE_RVTools_tabvInfo]

    GROUP BY 
        vInfoCluster
)

-- --------------------------------------------
-- Final Select:
-- Combine Cluster, Host, and VM aggregates into one view
-- --------------------------------------------

SELECT 
    cluster.vClusterName,  -- Cluster identifier

    -- Aggregated metrics with default 0 for missing values
    COALESCE(hostAgg.TotalCPUs, 0) AS nb_cpu,
    COALESCE(hostAgg.Nb_Hosts, 0) AS Nb_Hosts,
    COALESCE(vmAgg.Nb_VM, 0) AS Nb_VM,
    COALESCE(vmAgg.vInfoOS_Total, 0) AS vInfoOS_Total,
    COALESCE(vmAgg.vInfoOS_Windows, 0) AS vInfoOS_Windows,
    COALESCE(vmAgg.vInfoOS_Linux, 0) AS vInfoOS_Linux,
    COALESCE(vmAgg.vInfoOS_Others, 0) AS vInfoOS_Others,

    -- Include extracted and derived hardware information
    hostAgg.FirstWord,
    hostAgg.LastTwoSegments,
    hostAgg.CpuNum,
    hostAgg.CpuCore,
    hostAgg.Result

FROM 
    [VMWARE_RVTOOLS].[dbo].[VMWARE_RVTools_tabvCluster] AS cluster

-- Join with host aggregates on cluster name
LEFT JOIN 
    HostAggregates AS hostAgg
    ON cluster.vClusterName = hostAgg.vHostCluster

-- Join with VM aggregates on cluster name
LEFT JOIN 
    VMAggregates AS vmAgg
    ON cluster.vClusterName = vmAgg.vInfoCluster;
