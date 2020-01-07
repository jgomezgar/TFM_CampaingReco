/*################## 20 min #################################################*/
  

/*####################### 5 min ################################################*/
IF OBJECT_ID('[STAGING_2].[dbo].XXX_P_Sell_IN_Periods', 'U') IS NOT NULL
 DROP TABLE [STAGING_2].[dbo].XXX_P_Sell_IN_Periods;

 
select	SI.r -1 R,
		SI.CAL_DATE,
		SI.CAL_DATE_end,
		SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.Midcategory,
		SI.SI_ITG_WSE,
		ceiling(SUM(Mrkt_WSE_ajust + SI.SI_MRKT_ajust)) SI_MRKT_WSE

Into [STAGING_2].[dbo].XXX_P_Sell_IN_Periods
from [STAGING_2].[dbo].XXX_P_ITG_Sell_IN_Periods SI
join [STAGING_2].[dbo].XXX_P_Mrket_Sell_IN_ajust m
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

IF OBJECT_ID('[STAGING_2].[dbo].XXX_P_Sell_Periods_10d', 'U') IS NOT NULL
 DROP TABLE [STAGING_2].[dbo].XXX_P_Sell_Periods_10d;
 
 
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
		sum(SELLING_DAY) days_btw_order,
		ceiling(isnull(SI.SI_ITG_WSE/nullif(sum(SELLING_DAY),0),0)) DAILY_SI_ITG_WSE,
		ceiling(isnull(SI.SI_MRKT_WSE/nullif(sum(SELLING_DAY),0),0)) DAILY_SI_MRKT_WSE
from [STAGING_2].[dbo].XXX_P_Sell_IN_Periods SI
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

Into [STAGING_2].[dbo].XXX_P_Sell_Periods_10d
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
