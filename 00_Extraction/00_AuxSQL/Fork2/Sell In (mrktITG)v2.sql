/****** Script for SelectTopNRows command from SSMS  ******/
with ITG as (
SELECT [CUSTOMER_ID]
      ,b.SUBCATEGORY, b.BRANDFAMILY_ID
      ,[SALESDATE]
      ,[SALESMONTH]
      ,[SALESYEAR]
      ,[SALES_VOL]
  FROM [ITE_PRD].[ITE].[T_SMLD_CUSTPROD] f
  join ite.T_PRODUCTS p on  f.PRODUCT_ID = p.PRODUCT_ID
  join ite.T_BRANDPACKS b on  p.BRANDPACK_ID = b.BRANDPACK_ID  
   where CUSTOMER_ID <> '' and SALESMONTH >= '201709'-- desde 09 para que recoja la parte del 10 hasta fecha actual para cierres
   and b.SUBCATEGORY in ('Blond','RYO') and [SALES_VOL]<> 0
)

,MRKT_Full as (
 SELECT 
   --   row_number() over (partition by [CUSTOMER_ID], substring([SALESMONTH],5,2) order by sum([BLOND_VOL])) PosBlond,
   --   row_number() over (partition by [CUSTOMER_ID],substring([SALESMONTH],5,2) order by sum([RYO_VOL])) PosRYO,
   --   Count(*) over (partition by [CUSTOMER_ID], substring([SALESMONTH],5,2)) num,
      [SALESDATE]
  --    ,substring([SALESMONTH],5,2) YearMonth
    --  ,[SALESYEAR]
      ,[CUSTOMER_ID]
      ,([BLOND_VOL]) [BLOND_VOL]
--      ,AVG(sum([BLOND_VOL])) over (partition by [CUSTOMER_ID]) avg_blond
      ,([RYO_VOL]) [RYO_VOL]  
 --     ,AVG(sum([RYO_VOL])) over (partition by [CUSTOMER_ID]) avg_RYO
  FROM [ITE_PRD].[ITE].[T_SMLD_CUSTTOTALS]
  where CUSTOMER_ID <> '' and SALESMONTH >= '201709' --and SALESMONTH < convert(varchar(6),GETDATE()-1,112
 ---- and [BLOND_VOL] > 0
--  group by [SALESDATE],-- substring([SALESMONTH],5,2),
 -- [CUSTOMER_ID]
  )	
		
,MRKT as (
 SELECT 
      row_number() over (partition by [CUSTOMER_ID], substring([SALESMONTH],5,2) order by ([BLOND_VOL])) PosBlond,
      row_number() over (partition by [CUSTOMER_ID],substring([SALESMONTH],5,2) order by ([RYO_VOL])) PosRYO,
      Count(*) over (partition by [CUSTOMER_ID], substring([SALESMONTH],5,2)) num,
      [SALESDATE]
      ,substring([SALESMONTH],5,2) YearMonth
    --  ,[SALESYEAR]
      ,[CUSTOMER_ID]
      ,([BLOND_VOL]) [BLOND_VOL]
--      ,AVG(sum([BLOND_VOL])) over (partition by [CUSTOMER_ID]) avg_blond
      ,([RYO_VOL]) [RYO_VOL]  
 --     ,AVG(sum([RYO_VOL])) over (partition by [CUSTOMER_ID]) avg_RYO
  FROM [ITE_PRD].[ITE].[T_SMLD_CUSTTOTALS]
  where CUSTOMER_ID <> '' and SALESMONTH >= '201500' --and SALESMONTH < convert(varchar(6),GETDATE()-1,112
--  and [BLOND_VOL] > 0
--  group by [SALESDATE], substring([SALESMONTH],5,2),[CUSTOMER_ID]
  )		
		

, B_MRKT as (
select row_number() over (partition by MRKT.[CUSTOMER_ID] order by [SALESDATE]) R
,PosBlond
      ,[SALESDATE]
      ,MRKT.YearMonth
  --    ,[SALESYEAR]
      ,'BLOND' midcategory
      ,MRKT.[CUSTOMER_ID]
      ,[BLOND_VOL] MRKT_VOL
from MRKT join
     (select [CUSTOMER_ID], YearMonth, min([BLOND_VOL]) as q1, max([BLOND_VOL]) as q3, max([BLOND_VOL]) - min([BLOND_VOL]) as iqr
      from MRKT 
      
      where PosBlond = cast(num*0.35 as int) or PosBlond = cast(num*0.65 as int)
      group by [CUSTOMER_ID],YearMonth
     ) qs
     on qs.[CUSTOMER_ID] = MRKT.[CUSTOMER_ID] and qs.YearMonth = MRKT.YearMonth
 where --([BLOND_VOL]< q1 - 1.*iqr) or ([BLOND_VOL] > q3 + 1.*iqr)
 BLOND_VOL between q1 and q3
--order by customer_id, SALESDATE
)
, R_MRKT as (
select 
      row_number() over (partition by [CUSTOMER_ID] order by [SALESDATE]) R
      ,[SALESDATE]
 --     ,[SALESMONTH]
  --    ,[SALESYEAR]
      ,'RYO' midcategory
      ,[CUSTOMER_ID]
      ,[RYO_VOL] MRKT_VOL
From MRKT
--where [RYO_VOL] > avg_RYO*0.20 --and avg_RYO*1.75
)



Select m.R, m.[SALESDATE], s.[SALESDATE] SALESDATE_End, m.[CUSTOMER_ID],'BLOND' midcategory, sum(v.[BLOND_VOL]) MRKT_VOL

from B_MRKT m 
   join B_MRKT s 
     on m.[CUSTOMER_ID] = s.CUSTOMER_ID and
		m.R = s.R-1
    join MRKT_Full v
      on m.[CUSTOMER_ID] = v.CUSTOMER_ID and 
		v.[SALESDATE] >= m.[SALESDATE] and v.[SALESDATE]  < s.[SALESDATE]
where  s.[SALESDATE]	>	 '20171001'
group by m.R, m.[SALESDATE], s.[SALESDATE], m.[CUSTOMER_ID]

having sum(v.[BLOND_VOL]) < 0
    
/*
--select * from ITG f where exists 
--(
  
  select
/*   	m.SALESDATE,
	m.SALESMONTH,
	m.SALESYEAR,
	m.CUSTOMER_ID,
	m.BLOND_VOL,
	m.RYO_VOL, */
	i.CUSTOMER_ID,
	i.SUBCATEGORY,
--	i.BRANDFAMILY_ID,
	i.SALESDATE,
	i.SALESMONTH,
	i.SALESYEAR,
	sum(i.SALES_VOL),
	avg(i.SALES_VOL)
from
  MRKT m right outer join ITG I 
  on (m.[SALESDATE] = I.[SALESDATE] and m.CUSTOMER_ID=I.CUSTOMER_ID)
  where m.CUSTOMER_ID is null
 -- and (f.[SALESDATE] between I.[SALESDATE]-100 and I.[SALESDATE] and f.CUSTOMER_ID=I.CUSTOMER_ID)
group by
 /*  	m.SALESDATE,
	m.SALESMONTH,
	m.SALESYEAR,
	m.CUSTOMER_ID,
	m.BLOND_VOL,
	m.RYO_VOL, */
	i.CUSTOMER_ID,
	i.SUBCATEGORY,
--	i.BRANDFAMILY_ID,
	i.SALESDATE,
	i.SALESMONTH,
	i.SALESYEAR
Having sum(i.SALES_VOL) <> 0
--)
order by 1,4,3

*/

order by customer_id, SALESDATE

