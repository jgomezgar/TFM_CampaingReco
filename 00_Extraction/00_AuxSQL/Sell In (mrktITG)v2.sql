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
   where CUSTOMER_ID <> '' and SALESMONTH >= '201710'
   and b.SUBCATEGORY in ('Blond','RYO') and [SALES_VOL]<> 0
)

,MRKT as (
SELECT 
      [SALESDATE]
      ,[SALESMONTH]
      ,[SALESYEAR]
      ,[CUSTOMER_ID]
      ,[BLOND_VOL]
      ,[RYO_VOL]
  FROM [ITE_PRD].[ITE].[T_SMLD_CUSTTOTALS]
  where CUSTOMER_ID <> '' and SALESMONTH >= '201710'
  )
select * from ITG f where exists 
(
  
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
	sum(i.SALES_VOL)
from
  MRKT m right outer join ITG I 
  on (m.[SALESDATE] = I.[SALESDATE] and m.CUSTOMER_ID=I.CUSTOMER_ID)
  where m.CUSTOMER_ID is null
  and (f.[SALESDATE] between I.[SALESDATE]-100 and I.[SALESDATE] and f.CUSTOMER_ID=I.CUSTOMER_ID)
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
Having sum(i.SALES_VOL) = 0
)
order by 1,4,3