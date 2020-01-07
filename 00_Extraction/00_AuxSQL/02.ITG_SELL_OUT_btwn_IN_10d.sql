/*################### 20 min #################################################*/
  

/*####################### 5 min ################################################*/
IF OBJECT_ID('[STAGING_2].[dbo].XXX_Sell_IN_Periods', 'U') IS NOT NULL
 DROP TABLE [STAGING_2].[dbo].XXX_Sell_IN_Periods;

/*with tercio as( 
select *, 
 NTILE(3) OVER (
    PARTITION BY CAL_MONTH
    ORDER BY CAL_DATE DESC
) tercio
from ite.T_DAY
where SELLING_DAY = 1. and
CAL_MONTH between '201710' and convert(varchar(6),GETDATE()-1,112)
)

, selling_days as(
select CAL_MONTH, tercio,
--r= rank() over (order by CAL_MONTH, tercio desc),
--MIN(CAL_DATE)in_date ,
--MAX(CAL_DATE)EN_date,
case when tercio = 3 then DATEADD(DD,-DAY(MIN(CAL_DATE)) +1, MIN(CAL_DATE)) else MIN(CAL_DATE) end date_init, 
case when tercio = 1 then  dateadd(dd,-1,convert(date,convert(varchar(6),DATEADD(M,1,MAX(CAL_DATE)),112) + '01',112)) else MAX(CAL_DATE) end date_end,
SUM(SELLING_DAY) SELLING_DAY,
COUNT(*) dias
from tercio
group by CAL_MONTH, tercio)

, decenas as (
select * from selling_days s1 where tercio = 1 union all
select 	s1.CAL_MONTH,
	s1.tercio,
	s1.date_init,
	dateadd(dd,-1,s2.date_init) date_end,
	s1.SELLING_DAY,
	s1.dias
from 
selling_days s1 join selling_days s2
on s1.CAL_MONTH = s2.CAL_MONTH and s1.tercio  = s2.tercio +1
) */


  

 
select	SI.r -1 R,
		SI.CAL_DATE,
		SI.CAL_DATE_end,
		SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.Midcategory,
		SI.SI_ITG_WSE,
		ceiling(SUM(Mrkt_WSE_ajust + SI.SI_MRKT_ajust)) SI_MRKT_WSE

Into [STAGING_2].[dbo].XXX_Sell_IN_Periods
from [STAGING_2].[dbo].XXX_ITG_Sell_IN_Periods SI
join [STAGING_2].[dbo].XXX_Mrket_Sell_IN_ajust m
	   on SI.CUSTOMER_ID 	= m.CUSTOMER_ID and
		  SI.Midcategory =	m.Midcategory and
		  m.CAL_DATE between SI.CAL_DATE and SI.CAL_DATE_end  

where isnull([Mrkt_WSE],0) > 0
GROUP by 
		SI.r,
		SI.CAL_DATE,
		SI.CAL_DATE_end,
		SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.Midcategory,
		SI.SI_ITG_WSE
order by SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.CAL_DATE
		  


/*############################## 15 min ########################################*/

IF OBJECT_ID('[STAGING_2].[dbo].XXX_Sell_Periods_10d', 'U') IS NOT NULL
 DROP TABLE [STAGING_2].[dbo].XXX_Sell_Periods_10d;
 
 
with tercio as( 
select *, 
 NTILE(3) OVER (
    PARTITION BY CAL_MONTH
    ORDER BY CAL_DATE
) tercio
from ite.T_DAY
where SELLING_DAY = 1. and
CAL_MONTH between '201710' and convert(varchar(6),GETDATE()-1,112)
)

, selling_days as(
select CAL_MONTH, tercio,
--r= rank() over (order by CAL_MONTH, tercio desc),
--MIN(CAL_DATE)in_date ,
--MAX(CAL_DATE)EN_date,
case when tercio = 1 then DATEADD(DD,-DAY(MIN(CAL_DATE)) +1, MIN(CAL_DATE)) else MIN(CAL_DATE) end date_init, 
case when tercio = 3 then  dateadd(dd,-1,convert(date,convert(varchar(6),DATEADD(M,1,MAX(CAL_DATE)),112) + '01',112)) else MAX(CAL_DATE) end date_end,
SUM(SELLING_DAY) NUM_SELLING_DAYS
from tercio
group by CAL_MONTH, tercio)

, decenas as (
select * from selling_days s1 where tercio = 3 union all
select 	s1.CAL_MONTH,
	s1.tercio,
	s1.date_init,
	dateadd(dd,-1,s2.date_init) date_end,
	s1.NUM_SELLING_DAYS
from 
selling_days s1 join selling_days s2 
on s1.CAL_MONTH = s2.CAL_MONTH and s1.tercio  = s2.tercio -1
)
, M3 as (
select 
	d.CAL_DATE, 
	dec.CAL_MONTH,
	dec.tercio,
	dec.date_init,
	dec.date_end,
	convert(int,dec.NUM_SELLING_DAYS) NUM_SELLING_DAYS,
	DATEDIFF(DAY,dec.date_init, dec.date_end)+1 NUM_DAYS
from decenas dec 
join ite.T_DAY d 
 on d.CAL_DATE between date_init and date_end

)

,daily_Sell_IN_Periods as (

select	--SI.r -1 R,
		SI.CAL_DATE,
		SI.CAL_DATE_end,
		SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.Midcategory,
		SI.SI_ITG_WSE,
		SI.SI_MRKT_WSE,		
		--SI.SI_ITG_WSE /		SI.SI_MRKT_WSE SOM_P,
		sum(SELLING_DAY) days_btw_order,
		ceiling(isnull(SI.SI_ITG_WSE/nullif(sum(SELLING_DAY),0),0)) DAILY_SI_ITG_WSE,
		ceiling(isnull(SI.SI_MRKT_WSE/nullif(sum(SELLING_DAY),0),0)) DAILY_SI_MRKT_WSE
		--ceiling(SI.SI_ITG_WSE/sum(SELLING_DAY)) / ceiling(SI.SI_MRKT_WSE/sum(SELLING_DAY)) SOM
from [STAGING_2].[dbo].XXX_Sell_IN_Periods SI
join ite.T_DAY d on d.CAL_DATE between SI.CAL_DATE and	SI.CAL_DATE_end

group by
		SI.CAL_DATE,
		SI.CAL_DATE_end,
		SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.Midcategory,
		SI.SI_ITG_WSE,
		SI.SI_MRKT_WSE
)


  
, Sell_IN_Periods_10d as (
select	--SI.r -1 R,
		RANK() over (partition by SI.CUSTOMER_ID,SI.BRANDFAMILY_ID order by M3.date_init) R,
		M3.tercio,
		NUM_SELLING_DAYS,
		NUM_DAYS,
		ceiling(avg(days_btw_order)) days_btw_order,
		M3.date_init CAL_DATE,
		M3.date_end CAL_DATE_end,
		SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.Midcategory,
		ceiling(sum(d.SELLING_DAY*SI.DAILY_SI_ITG_WSE)) SI_ITG_WSE,
		ceiling(sum(d.SELLING_DAY*SI.DAILY_SI_MRKT_WSE)) SI_MRKT_WSE
from daily_Sell_IN_Periods SI
join ite.T_DAY d on d.CAL_DATE between SI.CAL_DATE and	SI.CAL_DATE_end
join M3  on M3.CAL_DATE = d.CAL_DATE

group by 		
		M3.tercio,
		NUM_SELLING_DAYS,
		NUM_DAYS,
		date_init,
		date_end,
		SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.Midcategory
)


, Sell_Periods_10d_ITG_SO as (
select	SI.r -1 R,
		SI.tercio,
		SI.NUM_SELLING_DAYS,
		SI.NUM_DAYS,
		SI.days_btw_order,
		SI.CAL_DATE,
		SI.CAL_DATE_end,
		SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.Midcategory,
		SI.SI_ITG_WSE,
		SI_MRKT_WSE,
  	ceiling(SUM( SO.SO_WSE)) SO_ITG_WSE

from Sell_IN_Periods_10d SI
left join [STAGING_2].[dbo].XXX_ITG_Sell_OUT SO 
	   on SI.CUSTOMER_ID 	= SO.CUSTOMER_ID and
		  SI.BRANDFAMILY_ID =	SO.[BRANDFAMILY_ID] and
		  SO_DATE between SI.CAL_DATE and SI.CAL_DATE_end
where  SI.CAL_DATE between SO_Start and SO_End
GROUP by 
		SI.r,
		SI.tercio,
		SI.NUM_SELLING_DAYS,
		SI.NUM_DAYS,
		SI.days_btw_order,
		SI.CAL_DATE,
		SI.CAL_DATE_end,
		SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.Midcategory,
		SI.SI_ITG_WSE,
		SI.SI_MRKT_WSE

)
		
, MRKT_SO as (
	select	
		a15.[CAL_DATE]  ,
		a11.[CUSTOMER_ID]  ,
		a11.[SUBCATEGORY]  Midcategory,
		sum(a11.[WSE_MRKT])  [WSE_MRKT]
	from	ITE.V_Fact_SMLD_WSE_SO_MRKT	a11
		join	ITE.T_DAY	a15
		  on 	(a11.[DIA] = a15.[DIA])
	where	a15.CAL_MONTH >=  '201710'
	  and a11.[SUBCATEGORY] in (N'BLOND', N'RYO')
	group by	a15.[CAL_DATE],
		a11.[CUSTOMER_ID],
		a11.[SUBCATEGORY]
)
		
select	SI.R,
		SI.tercio,
		SI.NUM_SELLING_DAYS,
		SI.NUM_DAYS,
		SI.days_btw_order,
		SI.CAL_DATE,
		SI.CAL_DATE_end,
		SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.Midcategory,
		SI.SI_ITG_WSE,
		SI.SI_MRKT_WSE,
  		SI.SO_ITG_WSE,
  		ceiling(SUM( SO.[WSE_MRKT])) SO_MRKT_WSE

Into [STAGING_2].[dbo].XXX_Sell_Periods_10d
from Sell_Periods_10d_ITG_SO SI
left join MRKT_SO SO 
	   on SI.CUSTOMER_ID 	= SO.CUSTOMER_ID and
		  SI.Midcategory =	SO.Midcategory and
		  SO.[CAL_DATE] between SI.CAL_DATE and SI.CAL_DATE_end

GROUP by 
		SI.r,
		SI.tercio,
		SI.NUM_SELLING_DAYS,
		SI.NUM_DAYS,
		SI.days_btw_order,
		SI.CAL_DATE,
		SI.CAL_DATE_end,
		SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.Midcategory,
		SI.SI_ITG_WSE,
		SI.SI_MRKT_WSE,
		SI.SO_ITG_WSE
order by SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.CAL_DATE 