SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Author: Landon Ochs 
Date: 8/12/19
Called By: SSIS Package - BSI ETL
Purpose: Molds the raw Quantum data into shape for the destination tables
=======================================================
Notes:

*/
CREATE VIEW [etl].[v_f_calco_Quote_split] AS

WITH QOH AS (
    SELECT
	   pt.NativeId
    FROM [dwh].[f_Inventory] i
    LEFT JOIN [dwh].[d_Part] pt
	   ON pt.PartId = i.PartId
    WHERE i.DivisionId = 1
    GROUP BY pt.NativeId
    HAVING SUM(QtyOnHand) > 0
)

SELECT
     [CQH_AUTO_KEY]								 AS NativeId
    ,[CQD_AUTO_KEY]								 AS NativeLineId
    ,ISNULL(cus.CustomerId, -1)					 AS CustomerId
    ,ISNULL(emp.EmployeeId, -1)					 AS EmployeeId
    ,ISNULL(pt.PartId, -1)						 AS PartId
    ,ISNULL(apt.PartId, -1)						 AS AltPartId
    ,ISNULL(srp.SalesRepId, -1)					 AS SalesRepId
    ,ae.DivisionId
    ,[CQ_NUMBER]								 AS QuoteNumber
    ,[ITEM_NUMBER]								 AS LineNumber
    ,CASE	  --Date logic fixes incorrectly entered dates that throw out of range errors in the cube
	   WHEN [DUE_DATE] < '10000101' THEN '19000101'
	   ELSE [DUE_DATE]
     END		 								 AS DueDate
    ,qt.[DATE_CREATED]								 AS DateCreated
    ,CASE
	   WHEN [ENTRY_DATE] < '10000101' THEN '19000101'
	   ELSE [ENTRY_DATE]
     END											AS QuoteDate
    ,CAST(CONVERT(char(8), IIF([ENTRY_DATE] < '10000101', '19000101', [ENTRY_DATE]), 112) as int)		AS QuoteDateId
    ,CASE
	   WHEN [SENT_DATE] < '10000101' THEN '19000101'
	   ELSE [SENT_DATE]
     END		 								 AS SentDate
    ,CAST([QTY_QUOTED] as float)	 				 AS QtyQuoted	 
    ,CAST([QTY_REQ] as float)						 AS QtyRequired
    ,CAST([UNIT_COST] as money)					 AS UnitCost
    ,CAST([UNIT_PRICE] as money)					 AS UnitPrice
    ,CAST([UNIT_PRICE] * [QTY_QUOTED] as money)		 AS LinePrice
    ,CAST([UNIT_COST] * [QTY_QUOTED] as money)		 AS LineCost
    ,[EXCHANGE_RATE]							 AS ExchangeRate
    ,CASE
	   WHEN [SO_FLAG] = 'T' THEN CAST(1 as bit)
	   ELSE CAST(0 as bit) 
	END										 AS SOFlag
    ,CASE
	   WHEN [SOD_LINK] = 'T' THEN CAST(1 as bit)
	   ELSE CAST(0 as bit) 
	END										 AS SOLinked
    ,CASE
	   WHEN [PRINTED] = 'T' THEN CAST(1 as bit)
	   ELSE CAST(0 as bit) 
	END										 AS IsPrinted
    ,CASE
	   WHEN [HISTORICAL_FLAG] = 'T' THEN CAST(1 as bit)
	   ELSE CAST(0 as bit) 
	END										 AS IsHistorical
    ,CASE
	   WHEN QOH.NativeId is null THEN 0
	   ELSE 1
     END										 AS HasQOH
	,pt.PartNumber
    ,GETDATE()						 AS ETLImportDate
    ,qt.dpt_auto_key
FROM [stg].[s_bsi_CQ_Detail] qt
LEFT JOIN [dwh].[d_Customer] cus
    ON qt.CMP_AUTO_KEY = cus.NativeId
    AND cus.DivisionId = 1
LEFT JOIN [dwh].[d_Employee] emp
    ON qt.SYSUR_AUTO_KEY = emp.NativeId
    AND emp.DivisionId = 1
LEFT JOIN [dwh].[d_Part] pt
    ON qt.PNM_AUTO_KEY = pt.NativeId
    AND pt.DivisionId = 1
LEFT JOIN [dwh].[d_Part] apt
    ON qt.ALT_PNM_AUTO_KEY = apt.NativeId
    AND apt.DivisionId = 1
LEFT JOIN [dwh].[d_SalesRep] srp
    ON ISNULL(qt.SPN_AUTO_KEY_LINE, qt.SPN_AUTO_KEY) = srp.NativeId
    AND srp.DivisionId = 1
LEFT JOIN QOH
    ON pt.NativeId = QOH.NativeId
LEFT JOIN [etl].[v_d_FDH_Aero_Entity] ae on ae.dpt_auto_key = qt.dpt_auto_key
where ae.DivisionId = 12





GO
