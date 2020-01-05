with mrkt as (SELECT 
      [SALESDATE]
      ,[SALESMONTH]
      ,[SALESYEAR]
      ,[CUSTOMER_ID]
      ,[BLOND_VOL]
      ,[RYO_VOL]
  FROM [ITE_PRD].[ITE].[T_SMLD_CUSTTOTALS]
  where CUSTOMER_ID <> '' and SALESMONTH >= '201709'
)
, neg as (
	SELECT 	m.SALESDATE,
	m.CUSTOMER_ID,
	sum(m.BLOND_VOL) BLOND_VOL 
FROM mrkt m  where [BLOND_VOL] < 0
group by 	m.SALESDATE,
	m.CUSTOMER_ID)
,Pedidos as (
select RANK() over (partition by [CUSTOMER_ID] order by  [SALESDATE]) R
,mrkt.*
,PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY m.SALESDATE)   
                            OVER (PARTITION BY [CUSTOMER_ID]) AS MedianCont  

,AVG(mrkt.[BLOND_VOL]) over (PARTITION by CUSTOMER_ID) [AVG_BLOND_VOL]
from mrkt where 
  [BLOND_VOL] > 0
 )
,Rango as ( 
 Select 	
    m.R,
    m.SALESDATE,
    p2.SALESDATE SALESDATE_end,
--	m.SALESMONTH,
--	m.SALESYEAR,
	m.CUSTOMER_ID,
	m.BLOND_VOL,
	m.[AVG_BLOND_VOL],
	MedianCont
--	m.RYO_VOL
 from Pedidos m join Pedidos p2
 on p2.R = m.R +1 and m.[CUSTOMER_ID] = p2.[CUSTOMER_ID]
 )
 
 select R.*, n.BLOND_VOL, n.SALESDATE , R.BLOND_VOL + ISNULL(n.blond_vol, 0) net, [AVG_BLOND_VOL],MedianCont
 from Rango R 
 left join neg n
 on R.customer_id =n.customer_id and
     n.SALESDATE >= R.SALESDATE and
     n.SALESDATE < R.SALESDATE_end
 where R.BLOND_VOL + ISNULL(n.blond_vol, 0) < [AVG_BLOND_VOL]*0.8 
 order by R.customer_id, R.R