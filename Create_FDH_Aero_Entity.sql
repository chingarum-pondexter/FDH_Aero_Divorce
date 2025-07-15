SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE view [etl].[v_d_FDH_Aero_Entity] as 

with overall as (
select 
dept.dept_name, 
div.division_name, 
syscm.company_name,
dept.DPT_AUTO_KEY,
case 
    when DPT_AUTO_KEY = 106 then 'Calco'
    when COMPANY_NAME = 'FDH AERO, LLC' and division_name = 'EMEA' then 'FDH Aero EMEA'
    when COMPANY_NAME = 'FDH AERO, LTD' then 'FDH Aero UK'
    when COMPANY_NAME = 'FDH AERO, LLC' and division_name = 'APAC' then 'FDH Aero APAC'
    when COMPANY_NAME = 'FDH AEROSPACE TECHNOLOGY (CHINA) CO., LTD.' then 'FDH China'
    when COMPANY_NAME = 'FDH AERO, LLC' then 'FDH Aero US'
    when COMPANY_NAME = 'Société AHE Inc.' then 'FDH Aero AHE'
    else syscm.company_name
    end as DEPT_Definition
from stg.s_bsi_DEPARTMENT dept 
join stg.s_bsi_DIVISION div on div.div_auto_key = dept.div_auto_key 
join stg.s_bsi_sys_Companies syscm on syscm.syscm_auto_Key = dept.syscm_auto_Key
)

select *, 
case
    when DEPT_Definition = 'FDH Aero US'    then 1
    when DEPT_Definition = 'FDH Aero AHE'   then 7
    when DEPT_Definition = 'FDH China'      then 9
    when DEPT_Definition = 'FDH Aero UK'    then 23
    when DEPT_Definition = 'FDH Aero APAC'  then 24
    when DEPT_Definition = 'FDH Aero EMEA'  then 25
    when DEPT_Definition = 'Calco'          then 12
    when COMPANY_NAME    = 'FDH AERO, LLC'     then 1      -- Catch all for other ungrouped items
    end as divisionid
from overall 

GO
