/* Hasta 9 Ultimas compras de la familia, minimo 5 compras*/
/*
with ITG_Sell_IN as (
select	--a12.[DIA]  DIA,
	  CAL_DATE,
	a11.[CUSTOMER_ID]  CUSTOMER_ID,
	a15.[BRANDFAMILY_ID]  BRANDFAMILY_ID,
	a15.SUBCATEGORY  Midcategory,
	sum(a11.[VOL_EQUI])  SI_WSE
from	ITE.FACT_SMLD_Smoke_ITG	a11
	join	ITE.T_DAY	a12
	  on 	(a11.[SALESDATE] = a12.[CAL_DAY])
	join	ITE.T_BRANDPACKS	a15
	  on 	(a11.[BRANDPACK_ID] = a15.[BRANDPACK_ID] )
	 
where	a12.CAL_MONTH >=  '201601' --and 201901
 and a15.SUBCATEGORY in (N'BLOND', N'RYO')
 and [VOL_EQUI] > 0
 and exists (select 1 from ITE.Fact_SO_Logista_smld_Custproduct	so where a11.[CUSTOMER_ID]=so.[CUSTOMER_ID] and so.[Mes_id] >=  201601 --and 201901
 )
group by	CAL_DATE,
	a11.[CUSTOMER_ID],
	a15.[BRANDFAMILY_ID],
	a15.SUBCATEGORY
)	

with	ITG_Sell_IN_top as (
select * 
from (
    select *, r = row_number() over (partition by [CUSTOMER_ID], [BRANDFAMILY_ID] order by CAL_DATE desc) 
    from [STAGING_2].[dbo].XXX_ITG_Sell_IN
    ) a 
where r <= 10
)
*/

select distinct * from (
select c.[CUSTOMER_ID], c.[BRANDFAMILY_ID], c.[Midcategory], 
	(DATEDIFF ( dd, CAL_DATE, CAL_DATE_end)) days_btwn_median,
	AVG(DATEDIFF ( dd, CAL_DATE, c.CAL_DATE_end)) over (partition by  c.[CUSTOMER_ID], c.[BRANDFAMILY_ID]) days_btwn_mean,
	c.r,
	pos = row_number() over (partition by c.[CUSTOMER_ID], c.[BRANDFAMILY_ID] order by DATEDIFF ( dd, CAL_DATE, c.CAL_DATE_end) desc ) ,
	round(0.51* MAX(c.r) over (partition by  c.[CUSTOMER_ID], c.[BRANDFAMILY_ID]),0) median_pos
from	[STAGING_2].[dbo].XXX_ITG_Sell_IN_Periods c
) a 
where pos= median_pos and median_pos>=3

order by 1,2

--select * from [STAGING_2].[dbo].XXX_ITG_Sell_IN_Periods
