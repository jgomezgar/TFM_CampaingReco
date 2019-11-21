with vending as (
select 
	a13.[MES_ID] MES,
	SUBSTRING(c.PROVINCE_TR_ID,1,2) PROV,
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

where [ITG_SV_vol] is not null and [Mrkt_SV_vol] > 0 and 
      a13.[MES_ID] < 201911 
)  


---#######################################
, CLTE AS (
select 
  MES, 
  CUSTOMER_ID,
  BRANDFAMILY_ID,	
  sum(ITG_SV_vol) ITG_SV_vol,
  sum(Mrkt_SV_vol) Mrkt_SV_vol,
  sum(ITG_SV_vol) / sum(Mrkt_SV_vol) clt_VShare
from vending
group by 
  MES, 
  CUSTOMER_ID,
  BRANDFAMILY_ID
)

---#######################################
, CP AS (
select 
	MES, 
	CP,
	BRANDFAMILY_ID,	
	SUM(ITG_SV_vol) ITG_SV_vol,
	SUM(Mrkt_SV_vol) Mrkt_SV_vol,
	sum(ITG_SV_vol) / sum(Mrkt_SV_vol) CP_VShare

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
---#######################################
, PROV AS (
select 
  MES, 
  PROV,
  BRANDFAMILY_ID,	
  sum(ITG_SV_vol) ITG_SV_vol,
  sum(Mrkt_SV_vol) Mrkt_SV_vol,
  sum(ITG_SV_vol) / sum(Mrkt_SV_vol) Prov_VShare
from vending
group by 
  MES, 
  PROV,
  BRANDFAMILY_ID
)
  
  SELECT c.[Customer_ID],
  	SUBSTRING(c.PROVINCE_TR_ID,1,2) PROV,
  	c.POSTALCODE CP,
  	[Mes_ID],
  	[Sales2C_num],
  	[predic2C],
  	clt_VShare,
  	cp_VShare,
  	Prov_VShare
  FROM [ITE_PRD].[ITE].[LU_CLTE_1CANAL] c
  left join [STAGING_2].[dbo].[XXX_Vending_Predic_Prop]v
  on v.CUSTOMER_ID=c.CUSTOMER_ID 
  left join CLTE 
	on (c.[Customer_ID] = CLTE.[Customer_ID]
		and CLTE.MES = v.Mes_ID)	
  left join CP 
	on c.POSTALCODE = CP.CP
	  and CP.MES = v.Mes_ID
  left join PROV 
	on PROV.PROV=SUBSTRING(c.PROVINCE_TR_ID,1,2)
		and PROV.MES = v.Mes_ID
  