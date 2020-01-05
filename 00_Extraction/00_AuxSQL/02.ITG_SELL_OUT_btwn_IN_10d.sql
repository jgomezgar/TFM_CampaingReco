/*################### 60 min #################################################*/
  

/*#########################################################################*/
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
case when tercio = 3 then DATEADD(DD,-DAY(MIN(CAL_DATE)) +1, MIN(CAL_DATE)) else MIN(CAL_DATE) end init_date, 
case when tercio = 1 then  dateadd(dd,-1,convert(date,convert(varchar(6),DATEADD(M,1,MAX(CAL_DATE)),112) + '01',112)) else MAX(CAL_DATE) end end_date,
SUM(SELLING_DAY) SELLING_DAY,
COUNT(*) dias
from tercio
group by CAL_MONTH, tercio)

, decenas as (
select * from selling_days s1 where tercio = 1 union all
select 	s1.CAL_MONTH,
	s1.tercio,
	s1.init_date,
	dateadd(dd,-1,s2.init_date) end_date,
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
		  m.CAL_DATE >= SI.CAL_DATE and
		  m.CAL_DATE < SI.CAL_DATE_end  

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
		  


/*#########################################################################*/
IF OBJECT_ID('[STAGING_2].[dbo].XXX_Sell_Periods_10d', 'U') IS NOT NULL
 DROP TABLE [STAGING_2].[dbo].XXX_Sell_Periods_10d;
 
 
with tercio as( 
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
case when tercio = 3 then DATEADD(DD,-DAY(MIN(CAL_DATE)) +1, MIN(CAL_DATE)) else MIN(CAL_DATE) end init_date, 
case when tercio = 1 then  dateadd(dd,-1,convert(date,convert(varchar(6),DATEADD(M,1,MAX(CAL_DATE)),112) + '01',112)) else MAX(CAL_DATE) end end_date,
SUM(SELLING_DAY) SELLING_DAY,
COUNT(*) dias
from tercio
group by CAL_MONTH, tercio)

, decenas as (
select * from selling_days s1 where tercio = 1 union all
select 	s1.CAL_MONTH,
	s1.tercio,
	s1.init_date,
	dateadd(dd,-1,s2.init_date) end_date,
	s1.SELLING_DAY,
	s1.dias
from 
selling_days s1 join selling_days s2 
on s1.CAL_MONTH = s2.CAL_MONTH and s1.tercio  = s2.tercio +1
)
, M3 as (
select d.CAL_DATE, dec.* from decenas dec join ite.T_DAY d on d.CAL_DATE between init_date and	end_date

)
  
, Sell_IN_Periods_10d as (
select	--SI.r -1 R,
		RANK() over (partition by SI.CUSTOMER_ID,SI.BRANDFAMILY_ID order by M3.init_date) R,
		M3.tercio
		M3.init_date CAL_DATE,
		M3.end_date CAL_DATE_end,
		SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.Midcategory,
		ceiling(sum(d.SELLING_DAY*SI.SI_ITG_WSE/M3.dias)) SI_ITG_WSE,
		ceiling(sum(d.SELLING_DAY*SI.SI_MRKT_WSE/M3.dias)) SI_MRKT_WSE
from [STAGING_2].[dbo].XXX_Sell_IN_Periods SI
join ite.T_DAY d on d.CAL_DATE between SI.CAL_DATE and	SI.CAL_DATE_end
join M3  on M3.CAL_DATE = d.CAL_DATE

group by 		
		M3.tercio
		init_date,
		end_date,
		SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.Midcategory
)

 
 
select	SI.r -1 R,
		SI.TERCIO
		SI.CAL_DATE,
		SI.CAL_DATE_end,
		SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.Midcategory,
		SI.SI_ITG_WSE,
		ceiling(SUM(SI_MRKT_WSE)) SI_MRKT_WSE,
   	    ceiling(SUM(case when SI.BRANDFAMILY_ID =	SO.BRANDFAMILY_ID  then SO.SO_WSE end)) SO_ITG_WSE,
  	    ceiling(SUM( SO.SO_WSE)) SO_MRKT_WSE

Into [STAGING_2].[dbo].XXX_Sell_Periods_10d
from Sell_IN_Periods_10d SI
left join [STAGING_2].[dbo].XXX_ITG_Sell_OUT SO 
	   on SI.CUSTOMER_ID 	= SO.CUSTOMER_ID and
		  SI.Midcategory =	SO.[SUBCATEGORY] and
		  SO_DATE > SI.CAL_DATE and
		  SO_DATE <= SI.CAL_DATE_end  

where  SI.CAL_DATE between SO_Start and SO_End

GROUP by 
		SI.r,
		SI.TERCIO
		SI.CAL_DATE,
		SI.CAL_DATE_end,
		SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.Midcategory,
		SI.SI_ITG_WSE
order by SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.CAL_DATE 