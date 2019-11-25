/***********   5 min *****************/
/* 
  [STAGING_2].[dbo].XXX_Mrkt_Sell_IN_neg --> Sell In Negativos, determina SubCategoria&Clitentes a arreglar
  [STAGING_2].[dbo].XXX_Mrket_Sell_IN_ajust  -->Sell In de estancos Sell Out, arreglados los negativos */

/*#########################################################################*/
--IF OBJECT_ID('[STAGING_2].[dbo].XXX_ITG_Sell_OUT', 'U') IS NOT NULL
-- DROP TABLE [STAGING_2].[dbo].XXX_ITG_Sell_OUT;





/*#########################################################################*/
--IF OBJECT_ID('[STAGING_2].[dbo].XXX_ITG_Sell_IN', 'U') IS NOT NULL
-- DROP TABLE [STAGING_2].[dbo].XXX_ITG_Sell_IN;


	
/*#########################################################################*/
IF OBJECT_ID('[STAGING_2].[dbo].XXX_Mrkt_Sell_IN_neg', 'U') IS NOT NULL
 DROP TABLE [STAGING_2].[dbo].XXX_Mrkt_Sell_IN_neg;
 
select	
	  CAL_DATE,
	a11.[CUSTOMER_ID]  CUSTOMER_ID,
	a11.[MIDCATEGORY]  ,
	ceiling(sum(a11.[Mrkt_WSE]))  [Mrkt_WSE]
into [STAGING_2].[dbo].XXX_Mrkt_Sell_IN_neg
from	[ITE_PRD].[ITE].[V_FACT_Sales_Target_WSE_Daily]	a11
	join	ITE.T_DAY	a12
	  on 	(a11.[DIA] = a12.[DIA])
where	a12.CAL_MONTH >=  '201710' and
 exists (select 1 from   [STAGING_2].[dbo].XXX_ITG_Sell_OUT SO 
				   where a11.CUSTOMER_ID 	= SO.CUSTOMER_ID and
					  a15.BRANDFAMILY_ID =	SO.BRANDFAMILY_ID and
					  a12.CAL_DATE between SO_Start and SO_End	 )
 and a11.MIDCATEGORY in (N'BLOND', N'RYO')
 and a11.[Mrkt_WSE] < 0
group by	CAL_DATE,
	a11.[CUSTOMER_ID],
	a11.[MIDCATEGORY]



/*#########################################################################*/
IF OBJECT_ID('[STAGING_2].[dbo].XXX_Mrket_Sell_IN_ajust', 'U') IS NOT NULL
 DROP TABLE [STAGING_2].[dbo].XXX_Mrket_Sell_IN_ajust;
 
 

			
with Mrkt_Sell_IN_std as (			
		select  CUSTOMER_ID,
				[MIDCATEGORY],
				Date_Start,Date_End,
				ceiling([Mrkt_WSE]) Mrkt_WSE_median,
				HI,std, mean
		from (
			select	t.DIA,
					t.MIDCATEGORY,
					t.CUSTOMER_ID,
					ceiling(t.ITG_WSE) ITG_WSE,
					ceiling(t.Mrkt_WSE) Mrkt_WSE,
 --n.CAL_DATE,n.SI_WSE,
						
				pos = row_number() over (partition by t.[CUSTOMER_ID], t.[MIDCATEGORY], n.CAL_DATE order by t.[Mrkt_WSE]) ,
				ceiling(max(t.[Mrkt_WSE]) over (partition by  t.[CUSTOMER_ID], t.[MIDCATEGORY],n.CAL_DATE)) HI,
				ceiling(0.5* count(*) over (partition by  t.[CUSTOMER_ID], t.[MIDCATEGORY],n.CAL_DATE)) median_pos,
				ceiling(STDEV(t.[Mrkt_WSE]) over (partition by  t.[CUSTOMER_ID], t.[MIDCATEGORY],n.CAL_DATE)) std,
				ceiling(avg(t.[Mrkt_WSE]) over (partition by  t.[CUSTOMER_ID], t.[MIDCATEGORY],n.CAL_DATE)) mean,
				min(d.CAL_DATE) over (partition by  t.[CUSTOMER_ID], t.[MIDCATEGORY],n.CAL_DATE) Date_Start,
				max(d.CAL_DATE) over (partition by  t.[CUSTOMER_ID], t.[MIDCATEGORY],n.CAL_DATE) Date_End
			from [ITE_PRD].[ITE].[V_FACT_Sales_Target_WSE_Daily] t
				join	ITE.T_DAY	d
				 on 	(t.[DIA] = d.[DIA])
			 join [STAGING_2].[dbo].XXX_Mrkt_Sell_IN_neg n 
				   on t.[CUSTOMER_ID]=n.[CUSTOMER_ID] and  
					  t.[MIDCATEGORY]= n.[MIDCATEGORY] and
					  n.CAL_DATE > d.CAL_DATE and
					  n.CAL_DATE < dateadd(m,2,d.CAL_DATE)
			where  t.MIDCATEGORY in (N'BLOND', N'RYO') and t.[Mrkt_WSE] > 0 and t.DIA > 20171000
					  
		) a
		where pos= median_pos	
)


/*### Normalizacion de Ventas negativas: 
Una venta negativa se elimina y 
provoca que la venta mas alta de los ultimos 2 meses 
se sustituya por la mediana */


select  --t.r R,
	d.CAL_DATE,--p.CAL_DATE CAL_DATE_end,
	t.[CUSTOMER_ID],
	t.[MIDCATEGORY],
	ceiling( t.[Mrkt_WSE]) [Mrkt_WSE],
	max(case when ceiling( t.[Mrkt_WSE]) = s.HI then Mrkt_WSE_median else ceiling(t.Mrkt_WSE) end) Mrkt_WSE_ajust,
	max(s.Mrkt_WSE_median) Mrkt_WSE_median, 
	s.HI, 
	avg(s.std) std, avg(s.mean) mean
into [STAGING_2].[dbo].XXX_Mrket_Sell_IN_ajust
from  [ITE_PRD].[ITE].[V_FACT_Sales_Target_WSE_Daily] t
				join	ITE.T_DAY	d
				 on 	(t.[DIA] = d.[DIA])	      
left join Mrkt_Sell_IN_std  s
	   on --n.CAL_DATE is not null and 
		  t.[CUSTOMER_ID]=s.[CUSTOMER_ID] and  
	      t.[MIDCATEGORY]= s.[MIDCATEGORY] and
	      d.CAL_DATE between Date_Start and Date_End
where  t.MIDCATEGORY in (N'BLOND', N'RYO') and t.[Mrkt_WSE] > 0 and t.DIA > 20171000
group by-- t.r ,
	d.CAL_DATE,--p.CAL_DATE,
	t.[CUSTOMER_ID],
	t.[MIDCATEGORY],
	ceiling(t.[Mrkt_WSE]) ,
	s.HI




