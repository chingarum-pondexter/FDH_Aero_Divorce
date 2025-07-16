SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





/*===========================================================
Author: Landon Ochs 
Date: 7/22/2020
Called By: SSIS - prc_BSIQuantum
Purpose: Molds the raw data into shape for the data warehouse
2025-01-06 Wayne Chia: Run on 250106 before changes produced 664 rows due to partial load of SM_HEADER.  Noticed dups on 2025-01-07, put in DISTINCT 
2025-01-10 Wayne Chia: Add in additional fields SMS_AUTO_KEY,SM_UDF_002,WHS_TO from SM_HEADER
2025-01-16 Wayne Chia: Fix missing ETLImportDate
Notes:
===========================================================*/

Create VIEW [etl].[v_f_calco_Shipment_split] AS

with overall as (
SELECT DISTINCT
    CAST(shp.[SMH_AUTO_KEY] AS INT) AS                                                              [NativeId],
	CAST(shp_de.[SMH_AUTO_KEY] AS INT) AS                                                              [NativeSODId]
   ,ISNULL([cst].[CustomerId],-1) AS                                                            [CustomerId]
   ,ISNULL([crc].[CurrencyId],-1) AS                                                            [CurrencyId]
   ,ISNULL([emp].[EmployeeId],-1) AS                                                            [EmployeeId]
   ,ae.DivisionId
   ,CAST([SOH_AUTO_KEY] AS INT) AS                                                              [NativeSOId]
   ,CAST([INH_AUTO_KEY] AS INT) AS                                                              [NativeInvoiceId]
   ,CAST([SVC_AUTO_KEY] AS INT) AS                                                              [NativeSvcAutoKey]
   ,CAST([SM_NUMBER] AS VARCHAR(75)) AS                                                         [ShipmentNumber]
   ,CAST([TRACKING_NUMBER] AS VARCHAR(100)) AS                                                  [TrackingNumber]
   ,CASE
	   WHEN [OPEN_FLAG] = 'T'
		  THEN CAST(1 AS BIT)
	   ELSE CAST(0 AS BIT)
    END AS                                                                                      [IsOpen]
   ,CASE	  --Date logic fixes incorrectly entered dates that throw out of range errors in the cube
	   WHEN [SHIP_DATE] < '10000101'
		  THEN '19000101'
	   ELSE [SHIP_DATE]
    END AS                                                                                      [ShipDate]
   ,CAST(CONVERT(CHAR(8),IIF([SHIP_DATE] < '10000101','19000101',[SHIP_DATE]),112) AS INT) AS   [ShipDateId]
   ,CASE	  --Date logic fixes incorrectly entered dates that throw out of range errors in the cube
	   WHEN shp.[ENTRY_DATE] < '10000101'
		  THEN '19000101'
	   ELSE shp.[ENTRY_DATE]
    END AS                                                                                      [EntryDate]
   ,CAST(CONVERT(CHAR(8),IIF(shp.[ENTRY_DATE] < '10000101','19000101',shp.[ENTRY_DATE]),112) AS INT) AS [EntryDateId]
   ,[inv].[InvoiceDate]
   ,[inv].[LinePrice] AS                                                                        [InvoicePrice]
   ,[inv].[InvoiceNumber]
   ,CAST([PACKER_NAME] AS VARCHAR(50)) AS                                                       [PackerName]
   ,CAST([SM_UDF_002] AS VARCHAR(10)) AS                                                        [WarehouseLocation]
   ,CAST([SHIP_PRIORITY] AS INT) AS                                                             [ShipmentPriority]
   ,[SMS_UPDATE] AS                                                                             [NativeModifiedDate]
   ,[DATE_CREATED] AS                                                                           [NativeCreatedDate]
   ,GETDATE() AS ETLImportDate
   ,[SMS_AUTO_KEY],SM_UDF_002,WHS_TO	--Added 250110 WC
   ,inv.dpt_auto_key
FROM [stg].[s_bsi_SM_Header] [shp]
inner join stg.s_bsi_SM_Detail shp_de
on shp.SMH_AUTO_KEY=shp_de.SMH_AUTO_KEY
LEFT JOIN [dwh].[d_Customer] [cst]
    ON [shp].[CMP_AUTO_KEY] = [cst].[NativeId]
	  AND [cst].[DivisionId] = 1
LEFT JOIN [dwh].[d_Employee] [emp]
    ON [shp].[SYSUR_AUTO_KEY] = [emp].[NativeId]
	  AND [emp].[DivisionId] = 1
LEFT JOIN
	( --Prevents dupliction when the invoice has more than 1 line
	    SELECT
		   [NativeId]
		  ,MAX([InvoiceDate]) AS   [InvoiceDate]
		  ,SUM([LinePrice]) AS     [LinePrice]
		  ,MAX([InvoiceNumber]) AS [InvoiceNumber]
          ,dpt_auto_key
	    FROM [dwh].[f_Invoice]
	    WHERE [DivisionId] = 1
	    GROUP BY
		   [NativeId],dpt_auto_key
	) [inv]
    ON [inv].[NativeId] = [shp].[INH_AUTO_KEY]
LEFT JOIN [dwh].[d_Currency] [crc]
    ON [crc].[NativeId] = [shp].[CUR_AUTO_KEY]
	  AND [crc].[DivisionId] = 1
left join etl.v_d_FDH_Aero_Entity ae ON    ae.DPT_AUTO_KEY = inv.DPT_AUTO_KEY
)

select *
from overall 
where divisionid = 12

GO
