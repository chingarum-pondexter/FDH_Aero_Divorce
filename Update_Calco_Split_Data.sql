--Update Calco Split Data 
update so 
set so.divisionid = ae.divisionid,
FDH_AERO_SPLIT = 1
FROM [dwh].[f_SalesOrder] so 
join [etl].[v_d_FDH_Aero_Entity] ae on ae.DPT_AUTO_KEY = so.DPT_AUTO_KEY
where ae.DEPT_Definition = 'Calco' and so.divisionid = 1
;

update inv 
set inv.divisionid = ae.divisionid,
FDH_AERO_SPLIT = 1
FROM [dwh].[f_Invoice] inv 
join [etl].[v_d_FDH_Aero_Entity] ae on ae.DPT_AUTO_KEY = inv.DPT_AUTO_KEY
where ae.DEPT_Definition = 'Calco' and inv.divisionid = 1
;

update inv 
set inv.divisionid = ae.divisionid,
FDH_AERO_SPLIT = 1
FROM [dwh].[f_Inventory] inv 
join [etl].[v_d_FDH_Aero_Entity] ae on ae.DPT_AUTO_KEY = inv.DPT_AUTO_KEY
where ae.DEPT_Definition = 'Calco' and inv.divisionid = 1
;

update po 
set po.divisionid = ae.divisionid,
FDH_AERO_SPLIT = 1
FROM [dwh].[f_PurchaseOrder] po 
join [etl].[v_d_FDH_Aero_Entity] ae on ae.DPT_AUTO_KEY = po.DPT_AUTO_KEY
where ae.DEPT_Definition = 'Calco' and po.divisionid = 1
;

update po 
set po.divisionid = ae.divisionid,
FDH_AERO_SPLIT = 1
FROM [dwh].[f_PurchaseOrderDetail] po 
join [etl].[v_d_FDH_Aero_Entity] ae on ae.DPT_AUTO_KEY = po.DPT_AUTO_KEY
where ae.DEPT_Definition = 'Calco' and po.divisionid = 1
;

update q 
set q.divisionid = ae.divisionid,
FDH_AERO_SPLIT = 1
FROM [dwh].[f_Quote] q 
join [etl].[v_d_FDH_Aero_Entity] ae on ae.DPT_AUTO_KEY = q.DPT_AUTO_KEY
where ae.DEPT_Definition = 'Calco' and q.divisionid = 1
;

update s 
set s.divisionid = ae.divisionid,
FDH_AERO_SPLIT = 1
FROM [dwh].[f_Shipment] s
join [etl].[v_d_FDH_Aero_Entity] ae on ae.DPT_AUTO_KEY = s.DPT_AUTO_KEY
where ae.DEPT_Definition = 'Calco' and s.divisionid = 1
;



