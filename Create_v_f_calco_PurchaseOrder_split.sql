SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create  VIEW [etl].[v_f_calco_PurchaseOrder_split]

AS    

with rec_dates as(
select distinct 
    po_number
    ,pod.item_number
    ,max(RC_DETAIL.ENTRY_DATE) as last_rec_date 
from 
    stg.s_bsi_RC_DETAIL RC_DETAIL 
INNER JOIN stg.s_bsi_RC_HEADER RC_HEADER  ON RC_HEADER.RCH_AUTO_KEY = RC_DETAIL.RCH_AUTO_KEY
inner join stg.s_bsi_po_detail as pod on RC_DETAIL.POD_AUTO_KEY = pod.POD_AUTO_KEY
group by po_number,pod.item_number
)

, cte as (  
SELECT    
     po.[POH_AUTO_KEY]         AS NativeId    
    ,[POD_AUTO_KEY]         AS NativeLineId    
    ,ISNULL(ven.VendorId, -1)       AS VendorId    
    ,ISNULL(emp.EmployeeId, -1)      AS EmployeeId    
    ,ISNULL(cus.CustomerId, -1)      AS CustomerId    
    ,ISNULL(whs.WarehouseId, -1)      AS WarehouseId    
    ,ISNULL(pt.PartId, -1)       AS PartId    
    ,ISNULL(apt.PartId, -1)       AS AltPartId    
    ,ISNULL(rte.RouteId, -1)       AS RouteId    
    ,po.DPT_AUTO_KEY
    ,ae.DivisionId
    /*,CASE 
		WHEN PO_Number LIKE '%DE%' OR PO_Number LIKE '%PUR%' THEN 7
		ELSE 1 
	END AS DivisionId*/
	-- ,1 AS DivisionId
    -- ,case 
    --     when LEFT(PO_NUMBER, 1) = 'C' AND LEFT(PO_NUMBER, 2) <> 'CM' AND LEFT(PO_NUMBER, 2) <> 'CA' then 9
    --     when LEFT(PO_NUMBER, 4) = 'CM-C' then 9 else 
	-- 1 end AS DivisionId	--recersed back 20241104   
    ,po.[PO_NUMBER]         AS PONumber    
    ,po.[ITEM_NUMBER]         AS LineNumber    
    ,CASE   --Date logic fixes incorrectly entered dates that throw out of range errors in the cube    
    WHEN po.[ENTRY_DATE] < '10000101' THEN '19000101'    
    ELSE po.[ENTRY_DATE]    
     END           AS PODate    
    ,CAST(CONVERT(char(8), IIF(po.[ENTRY_DATE] < '10000101', '19000101', po.[ENTRY_DATE]), 112) as int)  AS PODateId   
    ,CASE    
    WHEN rec.last_rec_date < '10000101' THEN '19000101'    
    ELSE rec.last_rec_date   --20240923 Anu changes from PO_SHIP_DATE to COMMIT_SHIP_DATE2
     END            AS ShipDate     
    -- ,CASE    
    -- WHEN po.COMMIT_SHIP_DATE2 < '10000101' THEN '19000101'    
    -- ELSE po.COMMIT_SHIP_DATE2   --20240923 Anu changes from PO_SHIP_DATE to COMMIT_SHIP_DATE2
    --  END            AS ShipDate    
 ,CASE    
    WHEN [NEXT_DELIVERY_DATE] < '10000101' THEN '19000101'    
    ELSE [NEXT_DELIVERY_DATE]    
     END           AS ETADate    
    -- ,[LAST_DELIVERY_DATE]       AS DeliveryDate    
    ,rec.[last_rec_date]       AS DeliveryDate    
    ,[QTY_BACK_ORDER]        AS QtyBackOrdered    ----These quantities represent the purchasing unit of measure
    ,[QTY_ORDERED]         AS QtyOrdered    ----These quantities represent the purchasing unit of measure
    ,[QTY_REC]          AS QtyReceived    
    --,CAST([QTY_ORDERED] - [QTY_REC] as float)   AS QtyOpen
		,CAST(([QTY_ORDERED]*[FACTOR]) - [QTY_REC] as float)   AS QtyOpen 
    --,CAST([UNIT_COST] as money)      AS UnitCost 
	,CAST([UNIT_COST]/[FACTOR] as money)      AS UnitCost  
    ,CAST([UNIT_COST] * [QTY_ORDERED] as money)   AS LineCost    
    ,CAST([VENDOR_PRICE] as money)      AS VendorPrice    
    ,CASE    
    WHEN po.OPEN_FLAG = 'T'
    THEN CAST(1 as bit)    
    ELSE CAST(0 as bit)     
 END           AS IsOpen    
    ,CASE    
    WHEN [RESALE_FLAG] = 'T' THEN CAST(1 as bit)    
    ELSE CAST(0 as bit)     
 END           AS IsResale    
    ,CASE    
    WHEN [DROP_SHIP] = 'T' THEN CAST(1 as bit)    
    ELSE CAST(0 as bit)     
 END           AS IsDropShip    
    ,po.[LAST_MODIFIED]        AS NativeModifiedDate    
    ,[LAST_MODIFIEDLINE]        AS NativeModifiedDateLine    
    ,po.[ETLImportDate]        AS ETLImportDate    
 ,pt.[UoM]  as UOM          
 ,po.[Remarks] as Remarks
 ,po.CNC_AUTO_KEY
 ,po.COMMIT_SHIP_DATE2
 ,SO_DEPT.Dept_Name as SO_DEPT_NAME
 ,PO_DEPT.Dept_Name as PO_DEPT_NAME
 ,gl.ACCOUNT_TYPE_DESC
 ,gl.DESCRIPTION
 ,case when warehouse_code like '%US%' then 'US'
 when warehouse_code like '%COE%' then 'US'
when warehouse_code like '%UK%' then 'UK' 
when warehouse_code like '%CN%' then 'China'
when warehouse_code = 'SHANGHAI' then 'China'
when warehouse_code = 'XAIC C919' then 'China'
when warehouse_code = 'XIAN LTA' then 'China'
when warehouse_code like '%DE%' then 'Bremen'
when warehouse_code = 'Germany' then 'Bremen'
when warehouse_code like '%VMI%' then 'VMI'
when warehouse_code like '%PDQ%' then 'PDQ'
else warehouse_code 
end as warehouse
FROM [stg].[s_bsi_PO_Detail] po  
LEFT JOIN stg.s_bsi_Warehouse as wh 
    ON po.WHS_AUTO_KEY = wh.WHS_AUTO_KEY
Left Join stg.s_bsi_AP_DETAIL as ap 
    ON po.POH_AUTO_KEY = ap.POH_AUTO_KEY
LEFT JOIN stg.s_bsi_AP_ACCOUNT as apc 
    ON ap.apa_auto_key = apc.apa_auto_key
LEFT JOIN stg.s_bsi_gl_account as gl 
    ON apc.gla_auto_key = gl.gla_auto_key and ap.poh_auto_key is not null
LEFT JOIN  STG.s_bsi_SO_header SOH 
    ON SOH.SO_NUMBER = po.PO_NUMBER
LEFT JOIN STG.s_bsi_DEPARTMENT AS SO_DEPT 
    ON SOH.DPT_AUTO_KEY =SO_DEPT.DPT_AUTO_KEY
LEFT JOIN STG.s_bsi_DEPARTMENT AS PO_DEPT 
    ON po.DPT_AUTO_KEY =PO_DEPT.DPT_AUTO_KEY
LEFT JOIN rec_dates as rec 
    ON po.po_number = rec.po_number
    and po.item_number = rec.item_number
LEFT JOIN [dwh].[d_Vendor] ven    
    ON po.CMP_AUTO_KEY = ven.NativeId    
    AND ven.DivisionId = 1    
LEFT JOIN [dwh].[d_Employee] emp    
    ON po.SYSUR_AUTO_KEY = emp.NativeId    
    AND emp.DivisionId = 1    
LEFT JOIN [dwh].[d_Customer] cus    
    ON po.CMP_BUYER = cus.NativeId    
    AND cus.DivisionId = 1    
LEFT JOIN [dwh].[d_Warehouse] whs    
    ON po.WHS_AUTO_KEY = whs.NativeId    
    AND whs.DivisionId = 1    
LEFT JOIN [dwh].[d_Part] pt    
    ON po.PNM_AUTO_KEY = pt.NativeId    
    AND pt.DivisionId = 1    
LEFT JOIN [dwh].[d_Part] apt    
    ON po.ALT_PNM_AUTO_KEY = apt.NativeId    
    AND apt.DivisionId = 1    
LEFT JOIN [dwh].[d_Route] rte    
    ON po.ROUTE_CODE = rte.RouteCode     
    AND rte.Source = 'Purchasing'    
    AND rte.DivisionId = 1 
LEFT JOIN etl.v_d_FDH_Aero_Entity ae on ae.DPT_AUTO_KEY = po.DPT_AUTO_KEY
    )

, final as (
    select *,ROW_NUMBER()over(partition by NativeId,NativeLineId order by NativeId) rn
    from cte
)

    select [NativeId]
      ,[NativeLineId]
      ,[VendorId]
      ,[EmployeeId]
      ,[CustomerId]
      ,[WarehouseId]
      ,[PartId]
      ,[AltPartId]
      ,[RouteId]
      ,[DPT_AUTO_KEY]
      ,[DivisionId]
      ,[PONumber]
      ,[LineNumber]
      ,[PODate]
      ,[PODateId]
      ,[ShipDate]
      ,try_cast(max(COMMIT_SHIP_DATE2) as date) as [ETADate]
      ,[DeliveryDate]
      ,[QtyBackOrdered]
      ,[QtyOrdered]
      ,[QtyReceived]
      ,[QtyOpen]
      ,[UnitCost]
      ,[LineCost]
      ,[VendorPrice]
      ,[IsOpen]
      ,[IsResale]
      ,[IsDropShip]
      ,[NativeModifiedDate]
      ,[NativeModifiedDateLine]
      ,[ETLImportDate]
      ,[UOM]
      ,[Remarks]
      ,[CNC_AUTO_KEY]
      ,[COMMIT_SHIP_DATE2]
      ,[SO_DEPT_NAME]
      ,[PO_DEPT_NAME]
      ,[ACCOUNT_TYPE_DESC]
      ,[DESCRIPTION]
      ,[warehouse]
      ,[rn]
    from final 
    where DivisionId = 12 and rn = 1
    group by [NativeId]
      ,[NativeLineId]
      ,[VendorId]
      ,[EmployeeId]
      ,[CustomerId]
      ,[WarehouseId]
      ,[PartId]
      ,[AltPartId]
      ,[RouteId]
      ,[DPT_AUTO_KEY]
      ,[DivisionId]
      ,[PONumber]
      ,[LineNumber]
      ,[PODate]
      ,[PODateId]
      ,[ShipDate]
      ,[ETADate]
      ,[DeliveryDate]
      ,[QtyBackOrdered]
      ,[QtyOrdered]
      ,[QtyReceived]
      ,[QtyOpen]
      ,[UnitCost]
      ,[LineCost]
      ,[VendorPrice]
      ,[IsOpen]
      ,[IsResale]
      ,[IsDropShip]
      ,[NativeModifiedDate]
      ,[NativeModifiedDateLine]
      ,[ETLImportDate]
      ,[UOM]
      ,[Remarks]
      ,[CNC_AUTO_KEY]
      ,[COMMIT_SHIP_DATE2]
      ,[SO_DEPT_NAME]
      ,[PO_DEPT_NAME]
      ,[ACCOUNT_TYPE_DESC]
      ,[DESCRIPTION]
      ,[warehouse]
      ,[rn]
;

GO
