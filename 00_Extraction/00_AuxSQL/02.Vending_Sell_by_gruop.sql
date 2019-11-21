with vending as (
select 
	a13.[MES_ID] MES,
	v.PROVINCE_ID,
	c.POSTALCODE,
	v.CP   CP,
	v.CUSTOMER_ID,
	Subcategory,
--	COMPANY,	
--	case when COMPANY = 'Competence' then COMPANY else  BRANDFAMILY_ID end BRANDFAMILY_ID,
	ceiling(sum([ITG_SV_vol])) [ITG_SV_vol],
	ceiling(sum([Mrkt_SV_vol])) [Mrkt_SV_vol]

from [STAGING_2].[dbo].XXX_Vending_Sell v 
join [ITE_PRD].[ITE].[LU_CLTE_1CANAL] c 
  on v.CUSTOMER_ID=c.CUSTOMER_ID
join ITE.V_Transf_MES_MQT1	a13
  on (v.[MES] = a13.[MQT1])

where a13.[MES_ID] < 201909
group by 
	a13.[MES_ID],
	v.PROVINCE_ID,
	c.POSTALCODE,
	v.CP,
	v.CUSTOMER_ID,
	Subcategory,
	COMPANY,	
	case when COMPANY = 'Competence' then COMPANY else  BRANDFAMILY_ID end
)

---#######################################
, CUSTOMER AS (
select 
	MES,
	PROVINCE_ID,
	POSTALCODE,
	CUSTOMER_ID,
	Subcategory,
	COMPANY,	
	BRANDFAMILY_ID,
	sum(Ventas_vol) Ventas_vol,
		nullIf(sum(sum(Ventas_vol)) over (partition by MES,POSTALCODE),0) Ventas_Tot_vol,
	sum(Ventas_vol) /
		nullIf(sum(sum(Ventas_vol)) over (partition by MES,POSTALCODE),0) Per_vol
from vending
group by 
	MES,
	PROVINCE_ID,
	POSTALCODE,
	CUSTOMER_ID,
	Subcategory,
	COMPANY,	
	BRANDFAMILY_ID 

)
---#######################################
, POSTALCODE AS (
select 
	MES,
--	PROVINCE_ID,
	POSTALCODE,
	Subcategory,
	COMPANY,	
	BRANDFAMILY_ID,
	sum(Ventas_vol) Ventas_vol,
		nullIf(sum(sum(Ventas_vol)) over (partition by MES,POSTALCODE),0) Ventas_Tot_vol,
	sum(Ventas_vol) /
		nullIf(sum(sum(Ventas_vol)) over (partition by MES,POSTALCODE),0) Per_vol
from (
	select MES,  POSTALCODE, Subcategory,COMPANY,BRANDFAMILY_ID,sum(Ventas_vol) Ventas_vol	from vending
	WHERE ISNUMERIC (POSTALCODE) = 1
	group by MES, PROVINCE_ID, POSTALCODE, Subcategory,COMPANY,BRANDFAMILY_ID
	UNION 
	select MES,  CP, Subcategory,COMPANY,BRANDFAMILY_ID, sum(Ventas_vol) Ventas_vol	from vending  
	WHERE ISNUMERIC (CP) = 1  AND CP <> POSTALCODE
	group by MES, PROVINCE_ID, CP, Subcategory,COMPANY,BRANDFAMILY_ID
) CP
		
group by 
	MES,
--	PROVINCE_ID,
	POSTALCODE,
	Subcategory,
	COMPANY,	
	BRANDFAMILY_ID
)



select 
	MES,
	PROVINCE_ID,
	Subcategory,
	COMPANY,	
	BRANDFAMILY_ID,
	sum(Ventas_vol) Ventas_vol,
		nullIf(sum(sum(Ventas_vol)) over (partition by MES,PROVINCE_ID),0) Ventas_Tot_vol,
	sum(Ventas_vol) /
		nullIf(sum(sum(Ventas_vol)) over (partition by MES,PROVINCE_ID),0) Per_vol
from vending
group by 
	MES,
	PROVINCE_ID,
	Subcategory,
	COMPANY,
	BRANDFAMILY_ID 
