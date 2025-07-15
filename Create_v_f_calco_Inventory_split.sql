SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


/*
Author: Landon Ochs 
Date: 8/15/19
Called By: SSIS Package - BSI ETL
Purpose: Molds the raw Quantum data into shape for the destination tables
=======================================================
Notes:
9/5/18 L. Ochs - Added an AgeId column for reporting
10/27/2021 - J. Burns added PartConditionId, TestRptCode, CertNumber, and Revision

*/
Create VIEW [etl].[v_f_calco_Inventory_split] AS

with overall as(
SELECT
     [STM_AUTO_KEY]									AS NativeId
    ,ISNULL(pt.PartId, -1)							AS PartId
    ,[IC_UDF_006]									AS LotNumber
    ,ISNULL(ven.VendorId, -1)						AS VendorId
    ,ISNULL(loc.LocationId, -1)						AS LocationId
    ,ISNULL(oloc.LocationId, -1)					AS OriginalLocationId
    ,ISNULL(whs.WarehouseId, -1)					AS WarehouseId
    ,ISNULL(owhs.WarehouseId, -1)					AS OriginalWarehouseId
    ,ISNULL(emp.EmployeeId, -1)						AS EmployeeId
    -- ,1												AS DivisionId
    -- ,case 
    --     when LEFT(ORIGINAL_PO_NUMBER, 1) = 'C' AND LEFT(ORIGINAL_PO_NUMBER, 2) <> 'CM' AND LEFT(ORIGINAL_PO_NUMBER, 2) <> 'CA' then 9
    --     when LEFT(ORIGINAL_PO_NUMBER, 4) = 'CM-C' then 9 else 
	-- 1 end AS DivisionId	
    ,inv.DPT_AUTO_KEY
    ,ae.divisionid
    ,ISNULL(age.AgeId, -1)							AS AgeId
    ,ISNULL(mfg.ManufacturerId, -1)					AS ManufacturerId
    ,[IND_AUTO_KEY]									AS NativeInvoiceLineId
    ,[POD_AUTO_KEY]									AS NativePOLineId
    ,[ORIGINAL_PO_NUMBER]							AS PONumber
    ,[REC_DATE]										AS ReceivedDate
    ,[QTY_REC]										AS QtyReceived
    ,[QTY_OH]										AS QtyOnHand
    ,[QTY_RESERVED]									AS QtyReserved
    ,[QTY_AVAILABLE]								AS QtyAvailable
    ,CAST([UNIT_COST] as money)						AS UnitCost
    ,CAST([UNIT_PRICE] as money)					AS UnitPrice
    ,CAST([UNIT_COST] * [QTY_OH] as money)			AS CurrentCost
    ,CAST([UNIT_PRICE] * [QTY_OH] as money)			AS CurrentValue
    ,CAST([UNIT_COST] * [QTY_REC] as money)			AS OriginalCost
    ,CAST([UNIT_PRICE] * [QTY_REC] as money)		AS OriginalValue
    ,CASE
	   WHEN [HISTORICAL_FLAG] = 'T' THEN CAST(1 as bit)
	   ELSE CAST(0 as bit)
    END												AS IsHistorical
    ,[CHANGE_TIMESTAMP]								AS NativeModifiedDate
    ,inv.[ETLImportDate]							AS ETLImportDate

    ,pc.PartConditionId								AS [PartConditionId]
    ,'N/A'											AS [TestRptCode]
    ,ISNULL(inv.PART_CERT_NUMBER, 'N/A')			AS [CertNumber]   
    ,ISNULL(inv.REVISION, 'N/A')			        AS [Revision]
FROM [stg].[s_bsi_Stock] inv
LEFT JOIN [dwh].[d_Vendor] ven
    ON inv.CMP_AUTO_KEY = ven.NativeId
    AND ven.Divisionid = 1
LEFT JOIN [dwh].[d_Employee] emp
    ON inv.SYSUR_AUTO_KEY = emp.NativeId
    AND emp.Divisionid = 1
LEFT JOIN [dwh].[d_Part] pt
    ON inv.PNM_AUTO_KEY = pt.NativeId
    AND pt.Divisionid = 1
LEFT JOIN [dwh].[d_Part] pta
    ON inv.PNM_AUTO_KEY = pta.NativeId
    AND pta.Divisionid = 1
LEFT JOIN [dwh].[d_Warehouse] whs
    ON inv.[WHS_AUTO_KEY] = whs.NativeId
    AND whs.Divisionid = 1
LEFT JOIN [dwh].[d_Location] loc
    ON inv.[LOC_AUTO_KEY] = loc.NativeId
    AND loc.Divisionid = 1
LEFT JOIN [dwh].[d_Warehouse] owhs
    ON inv.[WHS_ORIGINAL] = owhs.NativeId
    AND owhs.Divisionid = 1
LEFT JOIN [dwh].[d_Location] oloc
    ON inv.[LOC_ORIGINAL] = oloc.NativeId
    AND oloc.Divisionid = 1
LEFT JOIN [dwh].[d_Age] age
    ON DATEDIFF(month, [REC_DATE], GETDATE()) BETWEEN age.MinMonth AND age.MaxMonth
LEFT JOIN dwh.d_Manufacturer mfg
    ON inv.IC_UDF_005 = mfg.ManufacturerName
    AND mfg.Divisionid = 1
LEFT JOIN dwh.d_PartCondition pc
    ON inv.PCC_AUTO_KEY = pc.NativeId
    and pc.Divisionid = 1
LEFT JOIN [etl].[v_d_FDH_Aero_Entity] ae on ae.DPT_AUTO_KEY = inv.DPT_AUTO_KEY
)

, final as (

select *, ROW_NUMBER()over(partition by NativeId, DivisionId order by NativeId) rn
from overall

)

select *
from final
where DivisionId = 12 and rn = 1

GO
