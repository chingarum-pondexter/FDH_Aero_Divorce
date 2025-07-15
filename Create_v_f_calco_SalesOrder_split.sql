SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

Create VIEW [etl].[v_f_calco_SalesOrder_split] AS

WITH POs AS 
(
    SELECT
	   prt.NativeId AS PartAutoKey
	   ,UnitCost
	   ,ROW_NUMBER() OVER (PARTITION BY prt.NativeId ORDER BY PODate DESC) AS RN
    FROM dwh.f_PurchaseOrder po
    LEFT JOIN dwh.d_Part prt
	   ON prt.PartId = po.PartId
    WHERE po.Divisionid = 1
)
,PartClasses AS --Inventory Classes to assign sales to the class fromt he previous month to show what drove changes
(
    SELECT
	   AsOfDate
	   ,pt.PartNumber
	   ,ih.DivisionId
	   ,ProductClass
    FROM dwh.f_InventoryHistory ih
    LEFT JOIN [dwh].[d_Part] pt
	   ON pt.PartId = ih.PartId
    GROUP BY AsOfDate
	   ,pt.PartNumber
	   ,ih.DivisionId
	   ,ProductClass
)

,final as(
SELECT
     so.[SOH_AUTO_KEY]									AS NativeId
    ,[SOD_AUTO_KEY]									AS NativeLineId
    ,ISNULL(cus.CustomerId, -1)						AS CustomerId
    ,ISNULL(srp.SalesRepId, -1)						AS SalesRepId
    ,ISNULL(pt.PartId, -1)							AS PartId
    ,ISNULL(pta.PartId, -1)							AS AltPartId
    ,ISNULL(emp.EmployeeId, -1)						AS EmployeeId
    ,ISNULL(whs.WarehouseId, -1)						AS WarehouseId
    ,ISNULL(rte.RouteId, -1)							AS RouteId
    /*,CASE 
	    WHEN so.SO_NUMBER LIKE '%SMT%' OR so.SO_NUMBER LIKE '%SME%' THEN 7
		ELSE 1 
	END AS DivisionId	--Anu code based on incorrect instructions about splitting up AHE reporting*/
	-- ,case when LEFT(so.[SO_NUMBER], 2) = 'CA' then 9 else  1 end AS DivisionId	--recersed back 20241104
    -- ,case 
    --     when LEFT(so.SO_NUMBER, 1) = 'C' AND LEFT(so.SO_NUMBER, 2) <> 'CM' AND LEFT(so.SO_NUMBER, 2) <> 'CA' then 9
    --     when LEFT(so.SO_NUMBER, 4) = 'CM-C' then 9 else 
	-- 1 end AS DivisionId	--recersed back 20241104
    ,h.SYSCM_AUTO_KEY 
    ,h.DPT_AUTO_KEY
    ,bd.DIV_AUTO_KEY
	,ae.DivisionId
    ,so.[SO_NUMBER]									AS SONumber
    ,[ITEM_NUMBER]									AS LineNumber
    ,[CQD_AUTO_KEY]									AS NativeQuoteLineId
    ,CASE	  --Date logic fixes incorrectly entered dates that throw out of range errors in the cube
	   WHEN so.[ENTRY_DATE] < '10000101' THEN '19000101'
	   ELSE so.[ENTRY_DATE]
     END											AS OrderDate
    ,CAST(CONVERT(char(8), IIF(so.[ENTRY_DATE] < '10000101', '19000101', so.[ENTRY_DATE]), 112) as int)		AS OrderDateId
    ,CASE
	   WHEN [SHIP_DATE] < '10000101' THEN '19000101'
	   ELSE [SHIP_DATE]
     END		 									AS ShipDate
    ,CAST(CONVERT(char(8), IIF([SHIP_DATE] < '10000101', '19000101', [SHIP_DATE]), 112) as int)		AS ShipDateId
    --,CASE
	   --WHEN [DUE_DATE] < '10000101' THEN '19000101'
	   --ELSE [DUE_DATE]
    -- END		 	AS DueDate
	--switched on 9.17 v
    ,CASE
	   WHEN [DELIVERY_DATE] < '10000101' THEN '19000101'
	   ELSE [DELIVERY_DATE]
     END		 									AS DueDate
	 --switched on 9.17 ^
    ,CASE
	   WHEN [NEXT_SHIP_DATE] < '10000101' THEN '19000101'
	   ELSE [NEXT_SHIP_DATE]
     END											AS DeliveryDate
    ,CAST([UNIT_PRICE] as money)						AS UnitPrice
    ,ISNULL(NULLIF(CAST([UNIT_COST] as money), 0), POs.UnitCost)						AS UnitCost
    ,CAST([UNIT_PRICE] * QTY_ORDERED as money)			AS LinePrice
    ,CAST(ISNULL(NULLIF(CAST([UNIT_COST] as money), 0), POs.UnitCost) * QTY_ORDERED as money)			AS LineCost
    ,CAST([DISCOUNT] as money)						AS Discount
    ,CAST([QTY_ORDERED] as float)						AS QtyOrdered
    ,CAST([QTY_DELIVERED] as float)					AS QtyShipped
    ,CAST([QTY_INVOICED] as float)						AS QtyInvoiced
    ,CAST([QTY_RESERVED] as float)						AS QtyAllocated
    ,CAST([QTY_ORDERED] - QTY_DELIVERED as float)			AS QtyOpen
    ,so.[EXCHANGE_RATE]								AS ExchangeRate
    ,CASE
	   WHEN so.[OPEN_FLAG] = 'T' AND [QTY_ORDERED] - QTY_DELIVERED > 0
		  THEN CAST(1 as bit)
	   ELSE CAST(0 as bit)
    END											AS IsOpen
    ,so.[LAST_MODIFIED]								AS NativeModifiedDate
    ,[LAST_MODIFIED_LINE]							AS NativeModifiedDateLine
    ,czip.Country									AS BillingCountry
    ,czip.ZipCode									AS BillingZipCode
      ,CASE
        WHEN SUBSTRING(so.SO_NUMBER, 1, 3) LIKE '%L%' 
             AND LEN(SUBSTRING(so.SO_NUMBER, 1, 3)) = 3 
        THEN CAST(1 AS BIT)
        ELSE CAST(0 AS BIT)
    END AS IsLTA   -- added by anu ticket -- 44036 
    ,so.[ETLImportDate]								AS ETLImportDate
    ,ISNULL(pc.ProductClass, 'Z') AS ProductClass
    ,stat.[DESCRIPTION] AS [Status]
    ,so.CTD_AUTO_KEY as [CTD_AUTO_KEY]
    ,so.CTH_AUTO_KEY as [CTH_AUTO_KEY]
    ,ch.CONTRACT_NUMBER
    ,ROW_NUMBER()over(partition by so.[SOH_AUTO_KEY], [SOD_AUTO_KEY]  order by so.[ETLImportDate] desc) rownum


FROM [stg].[s_bsi_SO_Detail] so
LEFT JOIN [stg].[s_bsi_SO_Header] h 
    ON h.SOH_AUTO_KEY = so.SOH_AUTO_KEY  
LEFT JOIN stg.s_bsi_SO_Status stat 
    ON stat.SOS_AUTO_KEY = h.SOS_AUTO_KEY
left join stg.s_bsi_division bd on h.SYSCM_AUTO_KEY = bd.SYSCM_AUTO_KEY
LEFT JOIN [dwh].[d_Customer] cus
    ON so.CMP_AUTO_KEY = cus.NativeId
    AND cus.Divisionid = 1
LEFT JOIN [dwh].[d_Employee] emp
    ON so.SYSUR_AUTO_KEY = emp.NativeId
    AND emp.Divisionid = 1
LEFT JOIN [dwh].[d_Part] pt
    ON so.PNM_AUTO_KEY = pt.NativeId
    AND pt.Divisionid = 1
LEFT JOIN [dwh].[d_Part] pta
    ON so.ALT_PNM_AUTO_KEY = pta.NativeId
    AND pta.Divisionid = 1
LEFT JOIN [dwh].[d_SalesRep] srp
    ON ISNULL(so.SPN_AUTO_KEY_LINE, so.SPN_AUTO_KEY) = srp.NativeId
    AND srp.Divisionid = 1
LEFT JOIN [dwh].[d_Route] rte
    ON so.ROUTE_CODE = rte.RouteCode	
    AND rte.Source = 'Sales'
    AND rte.Divisionid = 1
LEFT JOIN [dwh].[d_Warehouse] whs
    ON so.[WHS_AUTO_KEY] = whs.NativeId
    AND whs.Divisionid = 1
	left join stg.s_bsi_Contract_Header ch

	on so.CTH_AUTO_KEY=ch.CTH_AUTO_KEY
CROSS APPLY etl.func_m_CountryZipMap(so.BILL_ADDRESS2, so.BILL_ADDRESS3, so.BILL_ADDRESS4, so.BILL_ADDRESS5) czip
LEFT JOIN POs
    ON POs.PartAutoKey = so.PNM_AUTO_KEY
    AND POs.RN = 1
LEFT JOIN PartClasses pc
    ON pt.PartNumber = pc.PartNumber
    AND pc.Divisionid = 1
    AND MONTH(DATEADD(month, -1, CAST([SHIP_DATE] as date))) = MONTH(pc.AsOfDate)
    AND YEAR(DATEADD(month, -1, CAST([SHIP_DATE] as date))) = YEAR(pc.AsOfDate)
LEFT JOIN etl.v_d_FDH_Aero_Entity ae on ae.DPT_AUTO_KEY = h.DPT_AUTO_KEY
)

select *, 
ROW_NUMBER() OVER (PARTITION BY SoNumber, NativeId ,NativeLineId,divisionid ORDER BY CAST(EtlImportDate AS DATE) desc) AS Rank
from final
where rownum = 1 and DivisionId = 12
;

GO
