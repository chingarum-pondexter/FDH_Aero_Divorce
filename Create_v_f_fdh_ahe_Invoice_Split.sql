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
20240904 - Nam/Wayne: Matt Lacki wants BSI revenue to be 100% matching Planful. Jose (Accounting) says Tax is not charged.  This will be removed and observation to be made if the 2% impact gets EDW closer to Planful for BSI Only.
*/
ALTER VIEW [etl].[v_f_fdh_ahe_Invoice_Split] AS

WITH Tax AS
(
    SELECT
	   INVC_NUMBER
	   ,COUNT(*) AS Lines
    FROM [stg].[s_bsi_Invc_Detail]i 
    left join etl.v_d_FDH_Aero_Entity ae on i.DPT_AUTO_KEY = ae.DPT_AUTO_KEY
    where DEPT_Definition = 'FDH Aero AHE'
    GROUP BY INVC_NUMBER
)

, updated as(
SELECT
     [INH_AUTO_KEY]								 AS NativeId
    ,[IND_AUTO_KEY]								 AS NativeLineId
    ,ISNULL(emp.EmployeeId, -1)					 AS EmployeeId
    ,ISNULL(cus.CustomerId, -1)					 AS CustomerId
    ,ISNULL(pt.PartId, -1)						 AS PartId
    ,ISNULL(srp.SalesRepId, -1)					 AS SalesRepId
    ,ISNULL(rte.RouteId, -1)						 AS RouteId
    ,ae.divisionid
    ,inv.[INVC_NUMBER]							 AS InvoiceNumber
    ,[ITEM_NUMBER]								 AS LineNumber
    ,[SOD_AUTO_KEY]								 AS NativeSOLineId
    ,[INVOICE_DATE]								 AS InvoiceDate
    ,CAST(CONVERT(char(8), [INVOICE_DATE], 112) as int) AS InvoiceDateId
    ,[ORDER_DATE]								 AS OrderDate
    ,CAST(CONVERT(char(8), [ORDER_DATE], 112) as int)	 AS OrderDateId
    ,[SHIP_DATE]								 AS ShipDate
    ,[POST_DATE]								 AS PostDate
    ,[POST_DESC]								 AS PostStatus
    ,[INVC_TYPE]								 AS InvoiceType
    ,[EXCHANGE_RATE]							 AS ExchangeRate
    ,CAST([QTY_SHIP] as float)			   		 AS QtyShipped
    ,CAST([QTY_BACK_ORDER] as float)		  		 AS QtyBackOrdered
    ,CAST([UNIT_COST] as money)					 AS UnitCost
    ,CAST([UNIT_PRICE] as money)					 AS UnitPrice
    ,CAST([UNIT_PRICE] * [QTY_SHIP] as money) /*+ (CAST([TAX_AMOUNT] as money)/tax.Lines)*/		 AS LinePrice
--    ,CAST([UNIT_PRICE] * [QTY_SHIP] as money) + (CAST([TAX_AMOUNT] as money)/tax.Lines)		 AS LinePrice	--Tax removed 240904
    ,CAST([UNIT_COST] * [QTY_SHIP] as money)			 AS LineCost
    ,CAST([TAX_AMOUNT] as money)/tax.Lines			 AS TaxAmount
    ,so.IsLTA									 AS IsLTA
    ,ae.DEPT_Definition
    ,czip.Country								 AS BillingCountry
    ,czip.ZipCode								 AS BillingZipCode
    ,inv.[ETLImportDate]							 AS ETLImportDate
FROM [stg].[s_bsi_Invc_Detail] inv
LEFT JOIN etl.v_d_FDH_Aero_Entity ae on inv.DPT_AUTO_KEY = ae.DPT_AUTO_KEY
LEFT JOIN [dwh].[d_Customer] cus
    ON inv.CMP_AUTO_KEY = cus.NativeId
    AND cus.DivisionId = 1
LEFT JOIN [dwh].[d_Employee] emp
    ON inv.SYSUR_AUTO_KEY = emp.NativeId
    AND emp.DivisionId = 1
LEFT JOIN [dwh].[d_Part] pt
    ON inv.PNM_AUTO_KEY = pt.NativeId
    AND pt.DivisionId = 1
LEFT JOIN [dwh].[d_SalesRep] srp
    ON inv.SPN_AUTO_KEY = srp.NativeId
    AND srp.DivisionId = 1
LEFT JOIN [dwh].[d_Route] rte
    ON inv.ROUTE_CODE = rte.RouteCode	
    AND rte.Source = 'Sales'
    AND rte.DivisionId = 1
LEFT JOIN [dwh].[f_SalesOrder] so
    ON so.NativeLineId = inv.SOD_AUTO_KEY
    AND so.DivisionId = 1
CROSS APPLY 
    etl.func_m_CountryZipMap(inv.BILL_ADDRESS2, inv.BILL_ADDRESS3, inv.BILL_ADDRESS4, inv.BILL_ADDRESS5) czip
LEFT JOIN Tax
    ON Tax.INVC_NUMBER = inv.INVC_NUMBER)

, final as (
    select *,ROW_NUMBER()OVER(partition by NativeId,NativeLineId , DivisionId order by NativeId) rn
    from updated 
)

select *
from final
where divisionid = 7 and 
rn = 1 and 
(DEPT_Definition = 'FDH Aero AHE')


GO
