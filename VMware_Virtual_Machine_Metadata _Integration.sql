/*This stored procedure consolidates and enriches VMware virtual machine (VM) data from multiple RVTools source tables, integrating additional metadata from METIS and CMDB sources. It performs cleanup, transformations, and grouping operations to generate a comprehensive summary table (table_VMWARE_RVTools_vinfo_metis) for reporting and analysis.
*/
USE [VMWARE]
GO

/***************************************************************
  Procedure: proc_VMWARE_RVTools_vinfo_metis
  Author:    Trang
  Date:      12/05/2025
  Purpose:   Consolidate and enrich VMware VM inventory data 
             from multiple sources (RVTools, METIS, CMDB).

  Description:
      - Reads VM details from RVTools source tables.
      - Cleans duplicates and enriches data with:
          * CPU, Disk, Datastore, Network, Tools info
          * CMDB cluster metadata
          * METIS server lifecycle attributes
      - Creates final aggregated dataset in:
            table_VMWARE_RVTools_vinfo_metis
***************************************************************/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[proc_VMWARE_RVTools_vinfo_metis]
AS
BEGIN

    -- Improve performance by reading uncommitted data
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    -------------------------------------------------------------------
    -- STEP 1: Create temporary working table with VM base information
    -------------------------------------------------------------------
    DROP TABLE IF EXISTS #temp1;

    SELECT
        [vInfoVMName],
        [vInfoPowerstate],
        [vInfoTemplate],
        [vInfoConfigStatus],
        [vInfoChangeVersion],
        [vInfoCreateDate],
        [vInfoCPUs],
        [vInfoMemory],
        [vInfoBootTime],
        [vInfoNICs],
        [vInfominRequiredEVCModeKey],
        [vInfoPrimaryIPAddress],
        [vInfoResourcepool],
        [vInfoFolder],
        [VInfoVersion],
        [vInfopath],
        [vInfoDataCenter],
        [vInfoCluster],
        [vInfoNumVirtualDisks],
        [vInfoInUse],
        [vInfoVMmonitoring],
        [vInfoNetwork1],
        [vInfoObjectID],
        [vInfoConnectionState],
        vInfoGuestState,
        UPPER(SUBSTRING(vInfoHost, 1, CHARINDEX('.', vInfoHost) -1)) AS vInfoHost,
        [vInfoOS],
        [vInfoOSTools],
        UPPER(SUBSTRING(vInfoVISDKServer, 1, CHARINDEX('.', vInfoVISDKServer) -1)) AS vInfoVISDKServer,
        [vInfo_tags_Environment],
        [vInfo_tags_Location],
        [vInfo_tags_Monitoring],
        [vInfo_tags_Customer],

        -- Convert storage and memory values to GB
        tabvInfo.vInfoUnshared / 1024 AS vInfoUnshared_Go,
        tabvInfo.vInfoMemory / 1024 AS vInfoMemory_Go,
        tabvInfo.vInfoProvisioned / 1024 AS vInfoProvisioned_Go,

        -- Extract Datastore name from path
        SUBSTRING(tabvInfo.vinfoPath,
                  CHARINDEX('[', tabvInfo.vinfoPath) + 1,
                  CHARINDEX(']', tabvInfo.vinfoPath) - CHARINDEX('[', tabvInfo.vinfoPath) - 1) AS vInfoDataStore,

        -- Initialize columns for later enrichment
        CAST(NULL AS VARCHAR(MAX)) AS vDiskCapacityMiB_Go,
        CAST(NULL AS VARCHAR(MAX)) AS vCPUCoresPerSocket,
        CAST(NULL AS VARCHAR(MAX)) AS vCPUSockets,
        CAST(NULL AS VARCHAR(MAX)) AS vDatastoreAddress,
        CAST(NULL AS VARCHAR(MAX)) AS vDataStoreCapacity_Go,
        CAST(NULL AS VARCHAR(MAX)) AS vDatastoreFreeSpace_Go,
        CAST(NULL AS VARCHAR(MAX)) AS vDataStoreFreePercentage,
        CAST(NULL AS VARCHAR(MAX)) AS Baie,
        CAST(NULL AS VARCHAR(MAX)) AS vNetworkIP4Address,
        CAST(NULL AS VARCHAR(MAX)) AS vToolsVersion,
        CAST(NULL AS VARCHAR(MAX)) AS clu_billing_function,
        CAST(NULL AS VARCHAR(MAX)) AS clu_high_availability,
        CAST(NULL AS VARCHAR(MAX)) AS metis_srv_checked_out_date,
        CAST(NULL AS VARCHAR(MAX)) AS metis_srv_hardware_status,
        CAST(NULL AS VARCHAR(MAX)) AS metis_srv_created_date,
        CAST(NULL AS VARCHAR(MAX)) AS metis_srv_support_group,

        -- Keep only one record per VM
        ROW_NUMBER() OVER (PARTITION BY vInfoVMName ORDER BY vInfoPowerstate DESC) AS row_num
    INTO #temp1
    FROM [VMWARE_RVTOOLS].[dbo].[VMWARE_RVTools_tabvInfo] AS tabvInfo;

    -------------------------------------------------------------------
    -- STEP 2: Join additional information from other RVTools tables
    -------------------------------------------------------------------

    -- Add CPU data
    UPDATE t
    SET
        vCPUCoresPerSocket = c.vCPUCoresPerSocket,
        vCPUSockets = c.vCPUSockets
    FROM #temp1 t
    LEFT JOIN [VMWARE_RVTOOLS].[dbo].[VMWARE_RVTools_tabvCPU] c
        ON t.vInfoVMName = c.vCPUVMName;

    -- Add Disk data
    UPDATE t
    SET vDiskCapacityMiB_Go = d.vDiskCapacityMiB / 1024
    FROM #temp1 t
    LEFT JOIN [VMWARE_RVTOOLS].[dbo].[VMWARE_RVTools_tabvDisk] d
        ON t.vInfoVMName = d.vDiskVMName;

    -- Add Datastore data
    UPDATE t
    SET
        vDatastoreAddress = ds.vDatastoreAddress,
        vDataStoreCapacity_Go = ds.vDataStoreCapacity / 1024,
        vDatastoreFreeSpace_Go = ds.vDatastoreFreeSpace / 1024,
        Baie = SUBSTRING(ds.vDatastoreAddress, 21, 4),
        vDataStoreFreePercentage = ds.vDataStoreFreePercentage
    FROM #temp1 t
    LEFT JOIN [VMWARE_RVTOOLS].[dbo].[VMWARE_RVTools_tabvDatastore] ds
        ON ds.vDatastoreName = SUBSTRING(t.vinfoPath,
                                         CHARINDEX('[', t.vinfoPath) + 1,
                                         CHARINDEX(']', t.vinfoPath) - CHARINDEX('[', t.vinfoPath) - 1);

    -- Add Network data
    UPDATE t
    SET vNetworkIP4Address = n.vNetworkIP4Address
    FROM #temp1 t
    LEFT JOIN [VMWARE_RVTOOLS].[dbo].[VMWARE_RVTools_tabvNetwork] n
        ON t.vInfoVMName = n.vNetworkVMName;

    -- Add Tools version
    UPDATE t
    SET vToolsVersion = tl.vToolsVersion
    FROM #temp1 t
    LEFT JOIN [VMWARE_RVTOOLS].[dbo].[VMWARE_RVTools_tabvTools] tl
        ON t.vInfoVMName = tl.vToolsVMName;

    -------------------------------------------------------------------
    -- STEP 3: Join CMDB and METIS cluster data
    -------------------------------------------------------------------

    -- Add Cluster info
    UPDATE t
    SET
        clu_billing_function = mc.u_billing_function,
        clu_high_availability = mc.u_high_availability
    FROM #temp1 t
    LEFT JOIN [mirror].[dbo].[cmdb_ci_cluster] mc
        ON t.vInfoCluster = mc.name;

    -- Add METIS server info
    UPDATE t
    SET
        metis_srv_checked_out_date = ms.metis_srv_checked_out_date,
        metis_srv_hardware_status = ms.metis_srv_hardware_status,
        metis_srv_created_date = ms.metis_srv_created_date,
        metis_srv_support_group = ms.metis_srv_support_group
    FROM #temp1 t
    LEFT JOIN [OPN_VIEW].[dbo].[METIS_SRV_ALL_OPN_DAILY] ms
        ON t.vInfoVMName = ms.metis_srv_name;

    -------------------------------------------------------------------
    -- STEP 4: Create final consolidated table
    -------------------------------------------------------------------
    DROP TABLE IF EXISTS table_VMWARE_RVTools_vinfo_metis;

    SELECT
        GETDATE() AS date_import,
        TRIM(vInfoVMName) AS vInfoVMName,
        [vInfoPowerstate],
        [vInfoTemplate],
        [vInfoConfigStatus],
        [vInfoChangeVersion],
        [vInfoCreateDate],
        [vInfoCPUs],
        -- CPU grouping
        CASE
            WHEN vInfoCPUs <= 3 THEN '0-3'
            WHEN vInfoCPUs <= 7 THEN '4-7'
            WHEN vInfoCPUs <= 11 THEN '8-11'
            WHEN vInfoCPUs <= 15 THEN '12-15'
            WHEN vInfoCPUs <= 19 THEN '15-19'
            WHEN vInfoCPUs <= 23 THEN '20-23'
            WHEN vInfoCPUs <= 32 THEN '24-32'
            WHEN vInfoCPUs <= 39 THEN '33-39'
            ELSE '>=40'
        END AS vInfoCPUs_groupes,
        [vInfoMemory],
        [vInfoBootTime],
        [vInfoNICs],
        [vInfominRequiredEVCModeKey],
        [vInfoPrimaryIPAddress],
        [vInfoResourcepool],
        [vInfoFolder],
        [VInfoVersion],
        [vInfopath],
        [vInfoDataCenter],
        [vInfoCluster],
        [vInfoNumVirtualDisks],
        [vInfoInUse],
        vInfoHost,
        [vInfoOS],
        [vInfoOSTools],
        [vInfoVMmonitoring],
        [vInfoNetwork1],
        [vInfoObjectID],
        [vInfoConnectionState],
        vInfoGuestState,
        vInfoVISDKServer,
        vInfo_tags_Environment,
        vInfo_tags_Location,
        vInfo_tags_Monitoring,
        vInfo_tags_Customer,
        vInfoUnshared_Go,
        vInfoMemory_Go,
        -- Memory grouping
        CASE
            WHEN vInfoMemory_Go < 2 THEN '0-2'
            WHEN vInfoMemory_Go < 4 THEN '2-4'
            WHEN vInfoMemory_Go < 8 THEN '4-8'
            WHEN vInfoMemory_Go < 16 THEN '8-16'
            WHEN vInfoMemory_Go < 24 THEN '16-24'
            WHEN vInfoMemory_Go < 32 THEN '24-32'
            WHEN vInfoMemory_Go < 48 THEN '32-48'
            WHEN vInfoMemory_Go < 64 THEN '48-64'
            WHEN vInfoMemory_Go < 96 THEN '64-96'
            WHEN vInfoMemory_Go < 128 THEN '96-128'
            WHEN vInfoMemory_Go < 192 THEN '128-192'
            WHEN vInfoMemory_Go < 256 THEN '192-256'
            ELSE '>=256'
        END AS vInfoMemory_Go_groupes,
        vInfoProvisioned_Go,
        -- Provisioned size grouping
        CASE
            WHEN vInfoProvisioned_Go < 100 THEN '50-100'
            WHEN vInfoProvisioned_Go < 250 THEN '100-250'
            WHEN vInfoProvisioned_Go < 500 THEN '250-500'
            WHEN vInfoProvisioned_Go < 750 THEN '500-750'
            WHEN vInfoProvisioned_Go < 1000 THEN '750-1000'
            WHEN vInfoProvisioned_Go < 1500 THEN '1000-1500'
            WHEN vInfoProvisioned_Go < 2000 THEN '1500-2000'
            WHEN vInfoProvisioned_Go < 4000 THEN '2000-4000'
            WHEN vInfoProvisioned_Go < 6000 THEN '4000-6000'
            WHEN vInfoProvisioned_Go < 8000 THEN '6000-8000'
            WHEN vInfoProvisioned_Go < 12000 THEN '8000-12000'
            WHEN vInfoProvisioned_Go < 16000 THEN '12000-16000'
            ELSE '>=16000'
        END AS vInfoProvisioned_Go_groupes,
        vInfoDataStore,
        vDiskCapacityMiB_Go,
        (CAST(vDiskCapacityMiB_Go AS FLOAT) + CAST(vInfoMemory_Go AS FLOAT)) / 1000 AS volumetrie_To,
        CASE
            WHEN (CAST(vDiskCapacityMiB_Go AS FLOAT) + CAST(vInfoMemory_Go AS FLOAT)) / 1000 > 9 THEN '>9To'
            WHEN (CAST(vDiskCapacityMiB_Go AS FLOAT) + CAST(vInfoMemory_Go AS FLOAT)) / 1000 > 6 THEN '>6To'
            ELSE '<6To'
        END AS volumetrie_groupe,
        vCPUCoresPerSocket,
        vCPUSockets,
        vDatastoreAddress,
        vDataStoreCapacity_Go,
        vDatastoreFreeSpace_Go,
        vDataStoreFreePercentage,
        Baie,
        vNetworkIP4Address,
        vToolsVersion,
        clu_billing_function,
        clu_high_availability,
        -- Compute coefficient based on cluster and billing type
        CASE
            WHEN clu_high_availability = 1 AND clu_billing_function = 'VMWARE SoCLOUD' THEN 2
            WHEN clu_high_availability = 0 AND clu_billing_function = 'VMWARE SoCLOUD' THEN 1
            WHEN clu_high_availability = 1 AND clu_billing_function <> 'VMWARE SoCLOUD' THEN 3
            WHEN clu_high_availability = 0 AND clu_billing_function <> 'VMWARE SoCLOUD' THEN 1.5
            ELSE 0
        END AS coef_applique,
        metis_srv_checked_out_date,
        -- Lifecycle grouping by months
        CASE
            WHEN DATEDIFF(DAY, metis_srv_checked_out_date, GETDATE()) / 30.4375 < 6 THEN '<6 mois'
            WHEN DATEDIFF(DAY, metis_srv_checked_out_date, GETDATE()) / 30.4375 <= 12 THEN '6 mois à 12 mois'
            WHEN DATEDIFF(DAY, metis_srv_checked_out_date, GETDATE()) / 30.4375 <= 18 THEN '12 mois à 18 mois'
            WHEN DATEDIFF(DAY, metis_srv_checked_out_date, GETDATE()) / 30.4375 <= 24 THEN '18 mois à 24 mois'
            ELSE '>24 mois'
        END AS Checked_out_month,
        metis_srv_hardware_status,
        metis_srv_created_date,
        CASE
            WHEN DATEDIFF(DAY, metis_srv_created_date, GETDATE()) / 30.4375 < 6 THEN '<6 mois'
            WHEN DATEDIFF(DAY, metis_srv_created_date, GETDATE()) / 30.4375 <= 12 THEN '6 mois à 12 mois'
            WHEN DATEDIFF(DAY, metis_srv_created_date, GETDATE()) / 30.4375 <= 18 THEN '12 mois à 18 mois'
            WHEN DATEDIFF(DAY, metis_srv_created_date, GETDATE()) / 30.4375 <= 24 THEN '18 mois à 24 mois'
            ELSE '>24 mois'
        END AS Created_month,
        metis_srv_support_group
    INTO table_VMWARE_RVTools_vinfo_metis
    FROM #temp1
    WHERE row_num = 1;

END
GO
