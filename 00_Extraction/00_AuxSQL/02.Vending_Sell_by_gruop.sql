with vending as (
select 
	a13.[MES_ID] MES,
	SUBSTRING(c.PROVINCE_TR_ID,1,2) T_PROV,
--	v.PROVINCE_ID V_PROV,
	c.POSTALCODE T_CP,
--	 SUBSTRING(v.CP,1,2) p,
	case when 
		len (v.CP) <> 5 OR 
		SUBSTRING(v.CP,1,2)<> v.PROVINCE_ID OR
		SUBSTRING(c.PROVINCE_TR_ID,1,2) <>	v.PROVINCE_ID
		then c.POSTALCODE else v.CP end   V_CP,
	v.CUSTOMER_ID,
	Subcategory,
    BRANDFAMILY_ID,
	ceiling([ITG_SV_vol]) [ITG_SV_vol],
	ceiling([Mrkt_SV_vol]) [Mrkt_SV_vol]

from [STAGING_2].[dbo].XXX_Vending_Sell v 
join [ITE_PRD].[ITE].[LU_CLTE_1CANAL] c 
  on v.CUSTOMER_ID=c.CUSTOMER_ID
-- ALISADO DE 3 MESES
join ITE.V_Transf_MES_MQT1	a13
  on (v.[MES] = a13.[MQT1])

where [ITG_SV_vol] is not null and a13.[MES_ID] < 201911 
)  


---#######################################
, CUSTOMER AS (

select 
	MES, 
	CP,
	BRANDFAMILY_ID,	
	SUM(ITG_SV_vol) ITG_SV_vol,
	SUM(Mrkt_SV_vol) Mrkt_SV_vol
		FROM (
			select 
				MES, 
				T_CP CP,
				BRANDFAMILY_ID,	
				(ITG_SV_vol) ITG_SV_vol,
				(Mrkt_SV_vol) Mrkt_SV_vol
			from vending
			where T_CP = V_CP
		union all
			select 
				MES, 
				V_CP,
				BRANDFAMILY_ID,	
				(ITG_SV_vol) ITG_SV_vol,
				(Mrkt_SV_vol) Mrkt_SV_vol
			from vending
			where T_CP <> V_CP
) CP
		group by 
			MES, 
			CP,
			BRANDFAMILY_ID
)

select 
  MES, 
  T_PROV,
  BRANDFAMILY_ID,	
  sum(ITG_SV_vol) ITG_SV_vol,
  sum(Mrkt_SV_vol) Mrkt_SV_vol
from vending
group by 
  MES, 
  T_PROV,
  BRANDFAMILY_ID