SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




/*
Author: Diksha 
Date: 5/4/2023

Purpose: Molds the raw Quantum data into shape for the destination tables
=======================================================
Notes:
CHi             12/13/24        Added the updated CTE to handle Dups
Raj Pipalia		07/20/2023		Changed the logic for quantity from RC_DETAIL.QTY_HIST_APPR to stock.QTY_REC based on discussion with Terrence H.
Raj Pipalia		07/24/2023		Updated the logic for Unit Cost and therefore Calculated value to use factor from PO Detail when the UOM_AUTO_KEY = 4 (bulk order Pounds), this will
								align the cost with the Quantity yielding the correct value
Raj Pipalia		07/25/2023		Updated the CASTING for Unit Cost such that the result is money data type
Raj Pipalia		08/29/2023		Updated with a band-aid to issues related to parts where the part was received incorrectly under a wrong UOM
*/

Create VIEW [etl].[v_f_calco_PurchaseOrderDetail_split] AS

with overall as (
SELECT 
     ISNULL(ven.VendorId, -1)													AS VendorId
    ,ISNULL(pt.PartId, -1)														AS PartId
	,CAST(PO_DETAIL.PO_NUMBER as varchar)										AS PONumber
	,RC_DETAIL.ITEM_NUMBER														AS LineNumber
	, sum
		(	CASE WHEN cast(stock.QTY_REC as float) >= 290000 AND PO_DETAIL.FACTOR <> 1 THEN cast(stock.QTY_REC/PO_DETAIL.FACTOR as float)
				ELSE cast(stock.QTY_REC as float)
			END
		)
																				AS QuantityReceived
	--sum(cast(stock.QTY_REC as float))											AS QuantityReceived
	,'U'																		AS Complete -- Unknown	
	,STOCK.REC_DATE																AS ReceivedDate 
	,CASE	WHEN PO_DETAIL.UOM_AUTO_KEY =4 AND PO_DETAIL.FACTOR <> 0 THEN CAST(PO_DETAIL.UNIT_COST 	/ PO_DETAIL.FACTOR	as money)					
			ELSE CAST(PO_DETAIL.UNIT_COST as money)	
			END																	AS UnitCost
	,SUM(CAST(( stock.QTY_REC * 
			CASE	WHEN PO_DETAIL.UOM_AUTO_KEY =4 AND PO_DETAIL.FACTOR <> 0 THEN CAST(PO_DETAIL.UNIT_COST as money)	/ PO_DETAIL.FACTOR							
			ELSE CAST(PO_DETAIL.UNIT_COST as money)	
			END) 
			AS FLOAT))															AS CalculatedValue 
    ,getdate()																	AS ETLImportDate
    -- ,case 
    --     when LEFT(PO_NUMBER, 1) = 'C' AND LEFT(PO_NUMBER, 2) <> 'CM' AND LEFT(PO_NUMBER, 2) <> 'CA' then 9
    --     when LEFT(PO_NUMBER, 4) = 'CM-C' then 9 else 
	-- 1 end AS DivisionId	
    -- ,case
    --     when RC_HEADER.DPT_AUTO_KEY = 3 and RC_HEADER.SYSCM_AUTO_KEY = 4 then 9
    --     when bd.DIV_AUTO_KEY = 6 then 9
    --     else 1 end as DivisionID 
    ,PO_DETAIL.DPT_AUTO_KEY
FROM   stg.s_bsi_RC_DETAIL RC_DETAIL 
INNER JOIN stg.s_bsi_RC_HEADER RC_HEADER 
	ON RC_DETAIL.RCH_AUTO_KEY = RC_HEADER.RCH_AUTO_KEY
INNER JOIN stg.s_bsi_STOCK STOCK 
	ON RC_DETAIL.RCD_AUTO_KEY = STOCK.RCD_AUTO_KEY
left join stg.s_bsi_DIVISION bd on RC_HEADER.SYSCM_AUTO_KEY = bd.SYSCM_AUTO_KEY
LEFT OUTER JOIN stg.s_bsi_PO_DETAIL PO_DETAIL 
	ON RC_DETAIL.POD_AUTO_KEY = PO_DETAIL.POD_AUTO_KEY
INNER JOIN stg.s_bsi_PARTS_MASTER PARTS_MASTER 
	ON STOCK.PNM_AUTO_KEY = PARTS_MASTER.PNM_AUTO_KEY
INNER JOIN stg.s_bsi_SYS_COMPANIES SYS_COMPANIES 
	ON RC_HEADER.SYSCM_AUTO_KEY = SYS_COMPANIES.SYSCM_AUTO_KEY
LEFT OUTER JOIN stg.s_bsi_COMPANIES COMPANIES 
	ON RC_HEADER.CMP_AUTO_KEY = COMPANIES.CMP_AUTO_KEY
LEFT JOIN [dwh].[d_Vendor] ven
	ON COMPANIES.CMP_AUTO_KEY = ven.NativeId
    AND ven.Divisionid = 1
LEFT JOIN [dwh].[d_Part] pt
    ON STOCK.PNM_AUTO_KEY = pt.NativeId
    AND pt.Divisionid = 1
WHERE PO_DETAIL.PO_NUMBER	is not null 
AND PO_DETAIL.UNIT_COST <> 0
group by VendorId,pt.Partid,PO_DETAIL.PO_NUMBER,RC_DETAIL.ITEM_NUMBER,STOCK.REC_DATE	,
			CASE	WHEN PO_DETAIL.UOM_AUTO_KEY =4 AND PO_DETAIL.FACTOR <> 0 THEN CAST(PO_DETAIL.UNIT_COST 	/ PO_DETAIL.FACTOR	 as money)						
					ELSE CAST(PO_DETAIL.UNIT_COST as money) end
			,PO_DETAIL.DPT_AUTO_KEY,RC_HEADER.SYSCM_AUTO_KEY,bd.DIV_AUTO_KEY)

, updated as(
    select *
 ,ROW_NUMBER()over(partition by vendorid, partid, linenumber order by vendorid) rn
    from overall 
 )   


select u.*,ae.divisionid
from updated u
join etl.v_d_FDH_Aero_Entity ae ON    ae.DPT_AUTO_KEY = u.DPT_AUTO_KEY
where rn = 1 and ae.divisionid = 12

GO
