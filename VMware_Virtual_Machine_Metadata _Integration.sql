/*This stored procedure consolidates and enriches VMware virtual machine (VM) data from multiple fictional source tables, integrating additional metadata from fictional cluster and lifecycle sources. It performs cleanup, transformations, and grouping operations to generate a comprehensive summary table (table_Fictional_VMware_VM_Info) for reporting and analysis.
*/
USE [Fictional_VMWARE_DB]
GO

/***************************************************************
  Procedure: proc_Fictional_VMware_VM_Info
  Purpose:   Consolidate and enrich VMware VM inventory data 
             from multiple fictional sources.

  Description:
      - Reads VM details from fictional VM inventory tables.
      - Cleans duplicates and enriches data with:
          * CPU, Disk, Datastore, Network, Tools info
          * Cluster metadata
          * Server lifecycle attributes
      - Creates final aggregated dataset in:
            table_Fictional_VMware_VM_Info
***************************************************************/

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[proc_Fictional_VMware_VM_Info]
AS
BEGIN

    -- Improve performance by reading uncommitted data
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    -------------------------------------------------------------------
    -- STEP 1: Create temporary working table with VM base information
    -------------------------------------------------------------------
    DROP TABLE IF EXISTS #temp_vm;

    SELECT
        [VMName],
        [PowerState],
        [TemplateFlag],
        [ConfigStatus],
        [ChangeVersion],
        [CreateDate],
        [CPUs],
        [Memory],
        [BootTime],
        [NICs],
        [MinRequiredEVC],
        [PrimaryIPAddress],
        [ResourcePool],
        [Folder],
        [Version],
        [Path],
        [DataCenter],
        [Cluster],
        [NumVirtualDisks],
        [InUseFlag],
        [VMMonitoring],
        [Network1],
        [ObjectID],
        [ConnectionState],
        GuestState,
        UPPER(SUBSTRING(HostName, 1, CHARINDEX('.', HostName) -1)) AS HostName,
        [OS],
        [OSTools],
        UPPER(SUBSTRING(VISDKServer, 1, CHARINDEX('.', VISDKServer) -1)) AS VISDKServer,
        [Tag_Environment],
        [Tag_Location],
        [Tag_Monitoring],
        [Tag_Customer],

        -- Convert storage and memory values to GB
        tab.VM_Unshared / 1024 AS VM_Unshared_GB,
        tab.VM_Memory / 1024 AS VM_Memory_GB,
        tab.VM_Provisioned / 1024 AS VM_Provisioned_GB,

        -- Extract Datastore name from path
        SUBSTRING(tab.Path,
                  CHARINDEX('[', tab.Path) + 1,
                  CHARINDEX(']', tab.Path) - CHARINDEX('[', tab.Path) - 1) AS DataStore,

        -- Initialize columns for enrichment
        CAST(NULL AS VARCHAR(MAX)) AS DiskCapacity_GB,
        CAST(NULL AS VARCHAR(MAX)) AS CPUCoresPerSocket,
        CAST(NULL AS VARCHAR(MAX)) AS CPUSockets,
        CAST(NULL AS VARCHAR(MAX)) AS DatastoreAddress,
        CAST(NULL AS VARCHAR(MAX)) AS DatastoreCapacity_GB,
        CAST(NULL AS VARCHAR(MAX)) AS DatastoreFreeSpace_GB,
        CAST(NULL AS VARCHAR(MAX)) AS DatastoreFreePct,
        CAST(NULL AS VARCHAR(MAX)) AS Bay,
        CAST(NULL AS VARCHAR(MAX)) AS NetworkIP,
        CAST(NULL AS VARCHAR(MAX)) AS ToolsVersion,
        CAST(NULL AS VARCHAR(MAX)) AS ClusterBillingFunction,
        CAST(NULL AS VARCHAR(MAX)) AS ClusterHighAvailability,
        CAST(NULL AS VARCHAR(MAX)) AS Server_CheckedOutDate,
        CAST(NULL AS VARCHAR(MAX)) AS Server_HardwareStatus,
        CAST(NULL AS VARCHAR(MAX)) AS Server_CreatedDate,
        CAST(NULL AS VARCHAR(MAX)) AS Server_SupportGroup,

        -- Keep only one record per VM
        ROW_NUMBER() OVER (PARTITION BY VMName ORDER BY PowerState DESC) AS row_num
    INTO #temp_vm
    FROM [Fictional_RVTOOLS_DB].[dbo].[VM_TabInfo] AS tab;

    -------------------------------------------------------------------
    -- STEP 2: Join additional information from other fictional tables
    -------------------------------------------------------------------

    -- CPU data
    UPDATE t
    SET
        CPUCoresPerSocket = c.CoresPerSocket,
        CPUSockets = c.Sockets
    FROM #temp_vm t
    LEFT JOIN [Fictional_RVTOOLS_DB].[dbo].[VM_TabCPU] c
        ON t.VMName = c.VMName;

    -- Disk data
    UPDATE t
    SET DiskCapacity_GB = d.DiskCapacity / 1024
    FROM #temp_vm t
    LEFT JOIN [Fictional_RVTOOLS_DB].[dbo].[VM_TabDisk] d
        ON t.VMName = d.VMName;

    -- Datastore data
    UPDATE t
    SET
        DatastoreAddress = ds.Address,
        DatastoreCapacity_GB = ds.Capacity / 1024,
        DatastoreFreeSpace_GB = ds.FreeSpace / 1024,
        Bay = SUBSTRING(ds.Address, 21, 4),
        DatastoreFreePct = ds.FreePct
    FROM #temp_vm t
    LEFT JOIN [Fictional_RVTOOLS_DB].[dbo].[VM_TabDatastore] ds
        ON ds.Name = SUBSTRING(t.Path,
                               CHARINDEX('[', t.Path) + 1,
                               CHARINDEX(']', t.Path) - CHARINDEX('[', t.Path) - 1);

    -- Network data
    UPDATE t
    SET NetworkIP = n.IPv4Address
    FROM #temp_vm t
    LEFT JOIN [Fictional_RVTOOLS_DB].[dbo].[VM_TabNetwork] n
        ON t.VMName = n.VMName;

    -- Tools version
    UPDATE t
    SET ToolsVersion = tl.Version
    FROM #temp_vm t
    LEFT JOIN [Fictional_RVTOOLS_DB].[dbo].[VM_TabTools] tl
        ON t.VMName = tl.VMName;

    -------------------------------------------------------------------
    -- STEP 3: Join fictional cluster and server lifecycle data
    -------------------------------------------------------------------

    -- Cluster info
    UPDATE t
    SET
        ClusterBillingFunction = c.BillingFunction,
        ClusterHighAvailability = c.HighAvailability
    FROM #temp_vm t
    LEFT JOIN [Fictional_CMDB_DB].[dbo].[Cluster_Info] c
        ON t.Cluster = c.Name;

    -- Server lifecycle info
    UPDATE t
    SET
        Server_CheckedOutDate = s.CheckedOutDate,
        Server_HardwareStatus = s.HardwareStatus,
        Server_CreatedDate = s.CreatedDate,
        Server_SupportGroup = s.SupportGroup
    FROM #temp_vm t
    LEFT JOIN [Fictional_METIS_DB].[dbo].[Server_Lifecycle] s
        ON t.VMName = s.VMName;

    -------------------------------------------------------------------
    -- STEP 4: Create final consolidated table
    -------------------------------------------------------------------
    DROP TABLE IF EXISTS table_Fictional_VMware_VM_Info;

    SELECT
        GETDATE() AS ImportDate,
        TRIM(VMName) AS VMName,
        PowerState,
        TemplateFlag,
        ConfigStatus,
        ChangeVersion,
        CreateDate,
        CPUs,
        Memory,
        BootTime,
        NICs,
        MinRequiredEVC,
        PrimaryIPAddress,
        ResourcePool,
        Folder,
        Version,
        Path,
        DataCenter,
        Cluster,
        NumVirtualDisks,
        InUseFlag,
        VMHost,
        OS,
        OSTools,
        VMMonitoring,
        Network1,
        ObjectID,
        ConnectionState,
        GuestState,
        VISDKServer,
        Tag_Environment,
        Tag_Location,
        Tag_Monitoring,
        Tag_Customer,
        VM_Unshared_GB,
        VM_Memory_GB,
        VM_Provisioned_GB,
        DiskCapacity_GB,
        CPUCoresPerSocket,
        CPUSockets,
        DatastoreAddress,
        DatastoreCapacity_GB,
        DatastoreFreeSpace_GB,
        DatastoreFreePct,
        Bay,
        NetworkIP,
        ToolsVersion,
        ClusterBillingFunction,
        ClusterHighAvailability,
        Server_CheckedOutDate,
        Server_HardwareStatus,
        Server_CreatedDate,
        Server_SupportGroup
    INTO table_Fictional_VMware_VM_Info
    FROM #temp_vm
    WHERE row_num = 1;

END
GO
