# VMware Cluster & VM Resource Analytics Project

## Overview
This project demonstrates advanced SQL data engineering and analytics skills applied to VMware infrastructure data. It consolidates, cleans, and analyzes virtual machine (VM) and cluster information to produce actionable insights on hardware, VM inventory, and resource utilization.

The focus is on creating reusable, scalable, and public-ready SQL scripts that can be applied in enterprise environments for reporting, capacity planning, and operational decision-making.

---

## Key Features

### VMware Sustainability & Cluster Mapping
- Consolidates server and hardware data into a unified view.  
- Maps VMware sustainability data to clusters and clients.  
- Resolves environment-specific company mapping for accurate reporting.

### VM Inventory Enrichment
- Integrates VM information from RVTools, CMDB, and METIS sources.  
- Calculates aggregated CPU, memory, disk, and datastore usage.  
- Groups VMs by OS, lifecycle stage, and resource size.  
- Computes weighted cluster capacity coefficients for operational analysis.

### Data Cleaning & Deduplication
- Identifies and removes duplicate service records based on server, application, and service name.  
- Uses SQL window functions (`ROW_NUMBER`) for precise deduplication logic.

### Dynamic Pivoting
- Creates dynamic pivot tables for vHBA (virtual Host Bus Adapter) information by host.  
- Transforms multiple rows per host into numbered columns to improve readability and reporting.

### Cluster-Level Summaries
- Aggregates CPU, memory, and VM counts by cluster.  
- Extracts manufacturer and model information from hosts for hardware signature analysis.  
- Groups VMs by operating system type and counts totals for each cluster.

### Project & Availability Resource Analysis
- Extracts project identifiers from VM metadata.  
- Classifies clusters as Monosite or Bisite based on high availability.  
- Computes total CPU cores, memory, and weighted capacity per project and site type.  
- Excludes decommissioned clusters to ensure accurate operational insights.

---

## Technical Highlights
- **SQL Server (T-SQL)** used for all transformations and aggregations.  
- Extensive use of:
  - Common Table Expressions (CTEs)  
  - Window functions (`ROW_NUMBER`, `SUM OVER`)  
  - Conditional logic and `CASE` statements  
  - Dynamic SQL for pivoting variable column sets  
- Integration of multiple data sources:
  - **RVTools** tables for VM and host info  
  - **CMDB** for cluster metadata  
  - **METIS** for server lifecycle data  
- Cleaned and anonymized code ready for public sharing.

---

## Skills Demonstrated
- Data modeling and normalization  
- SQL analytics and performance optimization  
- ETL (Extract, Transform, Load) pipeline design  
- Hardware and VM resource analysis  
- Problem-solving with complex logic for real-world IT infrastructure  
- Preparing professional, production-ready SQL scripts for portfolio/public sharing

---

## Project Structure
VMware_SQL_Portfolio/
│
├─ Views/
│ ├─ VMware_Sustainability_Client_Cluster_Mapping.sql
│ └─ Cluster_Level_Summary.sql
│
├─ StoredProcedures/
│ └─ proc_VMWARE_RVTools_vinfo_metis.sql
│
├─ Queries/
│ ├─ Remove_Duplicate_Services.sql
│ ├─ Dynamic_vHBA_Pivot.sql
│ └─ Project_Resource_Summary.sql
│
└─ README.md


---

## Outcome / Benefits
- Provides a single source of truth for VM and cluster resources.  
- Facilitates capacity planning, hardware lifecycle tracking, and cost allocation.  
- Helps IT teams identify underutilized resources and optimize workloads.  
- Demonstrates the ability to manage complex SQL projects and produce clean, scalable, shareable code.

---

## Notes
All database and table names in this repository have been anonymized and fictionalized for privacy.  

