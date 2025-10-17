/***************************************************************
  Title:     Cluster Resource Summary by Project and Availability Type (Fictional)
  Purpose:   Aggregate VM resource data by project and site redundancy.

  Description:
      - Parses the project reference (ProjectRef) to extract a "LIOS" identifier.
      - Classifies clusters as Monosite (single-site) or Bisite (dual-site)
        based on the HighAvailability flag.
      - Calculates:
            * Total CPU cores per project & site type
            * Total memory (in GB) per project & site type
            * Weighted capacity score (CapacityScore)
      - Filters out decommissioned clusters (InstallStatus != 'Retired')
***************************************************************/

WITH ProjectResourceSummary AS (
    SELECT DISTINCT
        -----------------------------------------------------------------
        -- Extract 'lios' project identifier from ProjectRef
        -----------------------------------------------------------------
        CONCAT(
            SUBSTRING(
                ProjectRef,
                CHARINDEX('-', ProjectRef, CHARINDEX('-', ProjectRef, CHARINDEX('-', ProjectRef) + 1) + 1) + 1,
                CHARINDEX('-', ProjectRef,
                    CHARINDEX('-', ProjectRef, CHARINDEX('-', ProjectRef, CHARINDEX('-', ProjectRef) + 1) + 1) + 1
                )
                - CHARINDEX('-', ProjectRef,
                    CHARINDEX('-', ProjectRef, CHARINDEX('-', ProjectRef) + 1) + 1
                ) - 1
            ),
            '-',
            RIGHT(
                ProjectRef,
                LEN(ProjectRef) - CHARINDEX('-',
                    ProjectRef,
                    CHARINDEX('-',
                        ProjectRef,
                        CHARINDEX('-',
                            ProjectRef,
                            CHARINDEX('-', ProjectRef) + 1
                        ) + 1
                    ) + 1
                )
            )
        ) AS LIOS,

        -----------------------------------------------------------------
        -- Determine site redundancy type: Monosite or Bisite
        -----------------------------------------------------------------
        CASE
            WHEN HighAvailability = '0' THEN 'Monosite'
            WHEN HighAvailability = '1' THEN 'Bisite'
            ELSE NULL
        END AS [Mono/Bisite],

        -----------------------------------------------------------------
        -- Total CPU cores per project & redundancy type
        -----------------------------------------------------------------
        SUM(CAST(VM_CPUs AS INT)) OVER (
            PARTITION BY CONCAT(
                SUBSTRING(ProjectRef,
                    CHARINDEX('-', ProjectRef, CHARINDEX('-', ProjectRef, CHARINDEX('-', ProjectRef) + 1) + 1) + 1,
                    CHARINDEX('-', ProjectRef,
                        CHARINDEX('-', ProjectRef, CHARINDEX('-', ProjectRef, CHARINDEX('-', ProjectRef) + 1) + 1) + 1
                    )
                    - CHARINDEX('-', ProjectRef,
                        CHARINDEX('-', ProjectRef, CHARINDEX('-', ProjectRef) + 1) + 1
                    ) - 1
                ),
                '-',
                RIGHT(
                    ProjectRef,
                    LEN(ProjectRef) - CHARINDEX('-',
                        ProjectRef,
                        CHARINDEX('-',
                            ProjectRef,
                            CHARINDEX('-',
                                ProjectRef,
                                CHARINDEX('-', ProjectRef) + 1
                            ) + 1
                        ) + 1
                    )
                ),
                '-',
                CASE
                    WHEN HighAvailability = '0' THEN 'Monosite'
                    WHEN HighAvailability = '1' THEN 'Bisite'
                    ELSE NULL
                END
            )
        ) AS Total_CPU,

        -----------------------------------------------------------------
        -- Total memory (GB) per project & redundancy type
        -----------------------------------------------------------------
        SUM(CAST(VM_Memory_GB AS FLOAT)) OVER (
            PARTITION BY CONCAT(
                SUBSTRING(ProjectRef,
                    CHARINDEX('-', ProjectRef, CHARINDEX('-', ProjectRef, CHARINDEX('-', ProjectRef) + 1) + 1) + 1,
                    CHARINDEX('-', ProjectRef,
                        CHARINDEX('-', ProjectRef, CHARINDEX('-', ProjectRef, CHARINDEX('-', ProjectRef) + 1) + 1) + 1
                    )
