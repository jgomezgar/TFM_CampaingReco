-- DEPRECATED

/*################### 6 min #################################################*/
  

/*#########################################################################*/
IF OBJECT_ID('[STAGING_2].[dbo].XXX_Sell_IN_Periods', 'U') IS NOT NULL
 DROP TABLE [STAGING_2].[dbo].XXX_Sell_IN_Periods;
  

 
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
IF OBJECT_ID('[STAGING_2].[dbo].XXX_Sell_Periods', 'U') IS NOT NULL
 DROP TABLE [STAGING_2].[dbo].XXX_Sell_Periods;
 
 
select	SI.r -1 R,
		SI.CAL_DATE,
		SI.CAL_DATE_end,
		SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.Midcategory,
		SI.SI_ITG_WSE,
		ceiling(SUM(SI_MRKT_WSE)) SI_MRKT_WSE,
   	    ceiling(SUM(case when SI.BRANDFAMILY_ID =	SO.BRANDFAMILY_ID  then SO.SO_WSE end)) SO_ITG_WSE,
  	    ceiling(SUM( SO.SO_WSE)) SO_MRKT_WSE

Into [STAGING_2].[dbo].XXX_Sell_Periods
from [STAGING_2].[dbo].XXX_Sell_IN_Periods SI
left join [STAGING_2].[dbo].XXX_ITG_Sell_OUT SO 
	   on SI.CUSTOMER_ID 	= SO.CUSTOMER_ID and
		  SI.Midcategory =	SO.[SUBCATEGORY] and
		  SO_DATE > SI.CAL_DATE and
		  SO_DATE <= SI.CAL_DATE_end  

where  SI.CAL_DATE between SO_Start and SO_End

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