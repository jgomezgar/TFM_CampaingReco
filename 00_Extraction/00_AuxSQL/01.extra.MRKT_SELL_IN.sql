-- DETECCION DE OUTLIER

--, B_MRKT as (
select row_number() over (partition by MRKT.[CUSTOMER_ID], MRKT.midcategory order by CAL_DATE) R
	  ,PosMonthSale
      ,CAL_DATE
      ,MRKT.YearMonth
  --    ,[SALESYEAR]
      ,MRKT.midcategory
      ,MRKT.[CUSTOMER_ID]
      ,[Mrkt_WSE_ajust] MRKT_VOL
      ,q1, q3
from [STAGING_2].[dbo].XXX_Mrket_Sell_IN_ajust MRKT join
     (select [CUSTOMER_ID], midcategory,YearMonth, min([Mrkt_WSE_ajust]) as q1, max([Mrkt_WSE_ajust]) as q3, max([Mrkt_WSE_ajust]) - min([Mrkt_WSE_ajust]) as iqr
      from [STAGING_2].[dbo].XXX_Mrket_Sell_IN_ajust MRKT 
      
      where PosMonthSale = cast(num*0.30 as int) or PosMonthSale = cast(num*0.95 as int)
      group by [CUSTOMER_ID],midcategory,YearMonth
     ) qs
     on qs.[CUSTOMER_ID] = MRKT.[CUSTOMER_ID] and qs.YearMonth = MRKT.YearMonth and qs.midcategory = MRKT.midcategory
 --where --([Mrkt_WSE_ajust]< q1 - 1.*iqr) or ([Mrkt_WSE_ajust] > q3 + 1.*iqr)
 --Mrkt_WSE_ajust between q1 and q3
--order by customer_id, SALESDATE
--)
order by MIDCATEGORY, CUSTOMER_ID, YearMonth, PosMonthSale


--select * from [STAGING_2].[dbo].XXX_Mrket_Sell_IN_ajust2 order by 		MIDCATEGORY, CUSTOMER_ID, YearMonth, PosMonthSale


4	22	2016-11-09	11	BLOND	01000017	86000
5	10	2016-11-10	11	BLOND	01000017	2400
6	13	2016-11-15	11	BLOND	01000017	11000
7	9	2016-11-24	11	BLOND	01000017	2000


select [CUSTOMER_ID], YearMonth, min([Mrkt_WSE_ajust]) as q1, max([Mrkt_WSE_ajust]) as q3, max([Mrkt_WSE_ajust]) - min([Mrkt_WSE_ajust]) as iqr
      from [STAGING_2].[dbo].XXX_Mrket_Sell_IN_ajust MRKT 
      
      where PosMonthSale = cast(num*0.30 as int) or PosMonthSale = cast(num*0.85 as int)
      group by [CUSTOMER_ID],YearMonth
order by 