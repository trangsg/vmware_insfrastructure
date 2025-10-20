-- ============================================
-- QUERY: Cluster-Level Summary of VMware Resources (Fictional)
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
        HostCluster,  -- Cluster name each host belongs to

        -- Total number of CPUs across all hosts in the cluster
        SUM(CAST(NumCPU AS int)) AS TotalCPUs,

        -- Count of unique hosts in the cluster
        COUNT(DISTINCT HostName) AS Nb_Hosts,

        -- Extract first word from manufacturer (e.g., "Dell" from "Dell Inc.")
        -- Skip if manufacturer name starts with "vmware"
        CASE
            WHEN LOWER(Manufacturer) LIKE 'vmware%' THEN NULL
            ELSE LEFT(Manufacturer, CHARINDEX(' ', Manufacturer + ' ') - 1)
        END AS FirstWord,

        -- Extract last segment from model name (after the last '-')
        -- Skip if model name starts with "vmware"
        CASE
            WHEN LOWER(Model) LIKE 'vmware%' THEN NULL
            ELSE REVERSE(LEFT(REVERSE(Model), CHARINDEX('-', REVERSE(Model) + '-') - 1))
        END AS LastTwoSegments,

        -- CPU info for each host
        CpuNum,
        CpuCore,

        -- Concatenate extracted manufacturer, model, and CPU details
        CONCAT(
            CASE
                WHEN LOWER(Manufacturer) LIKE 'vmware%' THEN ''
                ELSE LEFT(Manufacturer, CHARINDEX(' ', Manufacturer + ' ') - 1)
            END,
            '-',
            CASE
                WHEN LOWER(Model) LIKE 'vmware%' THEN ''
                ELSE REVERSE(LEFT(REVERSE(Model), CHARINDEX('-', REVERSE(Model) + '-') - 1))
            END,
            '-',
            CpuNum,
            '-',
            CpuCore
        ) AS Result

    FROM 
        Fictional_Hosts
),

-- --------------------------------------------
-- Subquery 2: VMAggregates
-- Calculates the number of VMs per cluster and
-- breaks them down by operating system.
-- --------------------------------------------
VMAggregates AS (
    SELECT 
        VMCluster,  -- Cluster name each VM belongs to

        -- Total number of distinct VMs per cluster
        COUNT(DISTINCT VMName) AS Nb_VM,

        -- Total count of OS entries (may include duplicates)
        COUNT(OS) AS OS_Total,

        -- Count of unique Windows VMs
        COUNT(DISTINCT CASE 
                           WHEN OS LIKE '%Windows%' THEN VMName
                      END) AS OS_Windows,

        -- Count of unique Linux VMs (specifically Red Hat)
        COUNT(DISTINCT CASE 
                           WHEN OS LIKE '%Red Hat%' THEN VMName
                      END) AS OS_Linux,

        -- Count of other OS types (not Windows or Red Hat)
        COUNT(DISTINCT CASE 
                           WHEN OS NOT LIKE '%Red Hat%' AND OS NOT LIKE '%Windows%' THEN VMName
                      END) AS OS_Others

    FROM 
        Fictional_VMs

    GROUP BY 
        VMCluster
)

-- --------------------------------------------
-- Final Select:
-- Combine Cluster, Host, and VM aggregates into one view
-- --------------------------------------------
SELECT 
    cluster.ClusterName,  -- Cluster identifier

    -- Aggregated metrics with default 0 for missing values
    COALESCE(hostAgg.TotalCPUs, 0) AS nb_cpu,
    COALESCE(hostAgg.Nb_Hosts, 0) AS Nb_Hosts,
    COALESCE(vmAgg.Nb_VM, 0) AS Nb_VM,
    COALESCE(vmAgg.OS_Total, 0) AS OS_Total,
    COALESCE(vmAgg.OS_Windows, 0) AS OS_Windows,
    COALESCE(vmAgg.OS_Linux, 0) AS OS_Linux,
    COALESCE(vmAgg.OS_Others, 0) AS OS_Others,

    -- Include extracted and derived hardware information
    hostAgg.FirstWord,
    hostAgg.LastTwoSegments,
    hostAgg.CpuNum,
    hostAgg.CpuCore,
    hostAgg.Result

FROM 
    Fictional_Clusters AS cluster

-- Join with host aggregates on cluster name
LEFT JOIN 
    HostAggregates AS hostAgg
    ON cluster.ClusterName = hostAgg.HostCluster

-- Join with VM aggregates on cluster name
LEFT JOIN 
    VMAggregates AS vmAgg
    ON cluster.ClusterName = vmAgg.VMCluster;
