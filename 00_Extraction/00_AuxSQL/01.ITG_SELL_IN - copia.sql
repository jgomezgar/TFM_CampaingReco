/***********   43 min *****************/
/*  RESULT: Datos entrenamiento ventas SellIn 
   [STAGING_2].[dbo].XXX_ITG_Sell_OUT  --> Sell Out WSE y fechas de validec de estancos, delita los erstancos Validos
   [STAGING_2].[dbo].XXX_ITG_Sell_IN --> Sell In Positivos, ventas base
   [STAGING_2].[dbo].XXX_ITG_Sell_IN_neg --> Sell In Negativos, determina Familias&Clitentes a arreglar
   [STAGING_2].[dbo].XXX_ITG_Sell_IN_Periods -->Sell In de estancos Sell Out, arreglados los negativos
*/

/*#########################################################################*/
IF OBJECT_ID('[STAGING_2].[dbo].XXX_ITG_Sell_OUT', 'U') IS NOT NULL
 DROP TABLE [STAGING_2].[dbo].XXX_ITG_Sell_OUT;


select	a16.[CAL_DATE] SO_DATE,
	a11.[CUSTOMER_ID]  CUSTOMER_ID,
	a13.[BRANDFAMILY_ID]  BRANDFAMILY_ID,
	a13.[SUBCATEGORY],
	sum(a11.[Volumen] / a110.[CONVERSION_FACTOR])  SO_WSE,
	min(a16.[CAL_DATE]) over (partition by a11.[CUSTOMER_ID], a13.[BRANDFAMILY_ID] ) SO_Start,
	max(a16.[CAL_DATE]) over (partition by a11.[CUSTOMER_ID], a13.[BRANDFAMILY_ID] ) SO_End
into [STAGING_2].[dbo].XXX_ITG_Sell_OUT
from	ITE.Fact_SO_Logista_smld_Custproduct	a11
	join	ITE.T_PRODUCTS	a12
	  on 	(a11.[Product_id] = a12.[Product_id])
	join	ITE.T_BRANDPACKS	a13
	  on 	(a12.[BRANDPACK_ID] = a13.[BRANDPACK_ID])
	join	ITE.T_DAY	a16
	  on 	(a11.[DIA] = a16.[DIA])
	join	ITE.fact_conversion_factor	a110
	  on 	(a13.[CATEGORY] = a110.[CATEGORY] and 
			a13.[EXPANDED] = a110.[EXPANDED] and 
			a13.[PACKTYPE] = a110.[PACKTYPE])
where a16.CAL_MONTH >=  '201601'
  and a13.[SUBCATEGORY] in (N'BLOND', N'RYO')
group by a16.[CAL_DATE],
		 a11.[CUSTOMER_ID],
		 a13.[BRANDFAMILY_ID],
		 a13.[SUBCATEGORY]


/*#########################################################################*/
IF OBJECT_ID('[STAGING_2].[dbo].XXX_ITG_Sell_IN', 'U') IS NOT NULL
 DROP TABLE [STAGING_2].[dbo].XXX_ITG_Sell_IN;
 
select	
	a12.CAL_DATE,
	a11.[CUSTOMER_ID]  CUSTOMER_ID,
	a15.[BRANDFAMILY_ID]  BRANDFAMILY_ID,
	a15.SUBCATEGORY  Midcategory,
	sum(a11.[VOL_EQUI])  SI_WSE
into [STAGING_2].[dbo].XXX_ITG_Sell_IN
from	ITE.FACT_SMLD_Smoke_ITG	a11
	join	ITE.T_DAY	a12
	  on 	(a11.[SALESDATE] = a12.[CAL_DAY])
	join	ITE.T_BRANDPACKS	a15
	  on 	(a11.[BRANDPACK_ID] = a15.[BRANDPACK_ID] )
	join   [STAGING_2].[dbo].XXX_ITG_Sell_OUT SO 
	   on a11.CUSTOMER_ID 	= SO.CUSTOMER_ID and
		  a15.BRANDFAMILY_ID =	SO.BRANDFAMILY_ID and
		  a12.CAL_DATE between SO_Start and SO_End
where	a12.CAL_MONTH >=  '201601' --and 201901
 and a15.SUBCATEGORY in (N'BLOND', N'RYO')
 and a11.[VOL_EQUI] > 0
group by	CAL_DATE,
	a11.[CUSTOMER_ID],
	a15.[BRANDFAMILY_ID],
	a15.SUBCATEGORY

	
/*#########################################################################*/
IF OBJECT_ID('[STAGING_2].[dbo].XXX_ITG_Sell_IN_neg', 'U') IS NOT NULL
 DROP TABLE [STAGING_2].[dbo].XXX_ITG_Sell_IN_neg;
 
select	
	  CAL_DATE,
	a11.[CUSTOMER_ID]  CUSTOMER_ID,
	a15.[BRANDFAMILY_ID]  BRANDFAMILY_ID,
	a15.SUBCATEGORY  Midcategory,
	ceiling(sum(a11.[VOL_EQUI]))  SI_WSE
into [STAGING_2].[dbo].XXX_ITG_Sell_IN_neg
from	ITE.FACT_SMLD_Smoke_ITG	a11
	join	ITE.T_DAY	a12
	  on 	(a11.[SALESDATE] = a12.[CAL_DAY])
	join	ITE.T_BRANDPACKS	a15
	  on 	(a11.[BRANDPACK_ID] = a15.[BRANDPACK_ID] )
	join   [STAGING_2].[dbo].XXX_ITG_Sell_OUT SO 
	   on a11.CUSTOMER_ID 	= SO.CUSTOMER_ID and
		  a15.BRANDFAMILY_ID =	SO.BRANDFAMILY_ID and
		  a12.CAL_DATE between SO_Start and SO_End 
where	a12.CAL_MONTH >=  '201601' --and 201901
 and a15.SUBCATEGORY in (N'BLOND', N'RYO')
 and a11.[VOL_EQUI] < 0
group by	CAL_DATE,
	a11.[CUSTOMER_ID],
	a15.[BRANDFAMILY_ID],
	a15.SUBCATEGORY



/*#########################################################################*/
IF OBJECT_ID('[STAGING_2].[dbo].XXX_ITG_Sell_IN_Periods', 'U') IS NOT NULL
 DROP TABLE [STAGING_2].[dbo].XXX_ITG_Sell_IN_Periods;
 
 
with ITG_Sell_IN_top as (
			select * 
			from (
				select *, r = row_number() over (partition by [CUSTOMER_ID], [BRANDFAMILY_ID] order by CAL_DATE desc), 
				COUNT( CAL_DATE) over (partition by [CUSTOMER_ID], [BRANDFAMILY_ID]) num
				from [STAGING_2].[dbo].XXX_ITG_Sell_IN
				) a 
			where -- r <= 10 and 
			   num >=6
			)
			
,ITG_Sell_IN_std as (			
		select  CUSTOMER_ID,
				BRANDFAMILY_ID,
				Date_Start,Date_End,
				ceiling(SI_WSE) SI_WSE_median,
				HI,std, mean
		from (
			select t.*, --n.CAL_DATE,n.SI_WSE,
						
				pos = row_number() over (partition by t.[CUSTOMER_ID], t.[BRANDFAMILY_ID], n.CAL_DATE order by t.SI_WSE) ,
				ceiling(max(t.SI_WSE) over (partition by  t.[CUSTOMER_ID], t.[BRANDFAMILY_ID],n.CAL_DATE)) HI,
				ceiling(0.5* count(*) over (partition by  t.[CUSTOMER_ID], t.[BRANDFAMILY_ID],n.CAL_DATE)) median_pos,
				ceiling(STDEV(t.SI_WSE) over (partition by  t.[CUSTOMER_ID], t.[BRANDFAMILY_ID],n.CAL_DATE)) std,
				ceiling(avg(t.SI_WSE) over (partition by  t.[CUSTOMER_ID], t.[BRANDFAMILY_ID],n.CAL_DATE)) mean,
				min(t.CAL_DATE) over (partition by  t.[CUSTOMER_ID], t.[BRANDFAMILY_ID],n.CAL_DATE) Date_Start,
				max(t.CAL_DATE) over (partition by  t.[CUSTOMER_ID], t.[BRANDFAMILY_ID],n.CAL_DATE) Date_End
			from [STAGING_2].[dbo].XXX_ITG_Sell_IN t
			 join [STAGING_2].[dbo].XXX_ITG_Sell_IN_neg n 
				   on t.[CUSTOMER_ID]=n.[CUSTOMER_ID] and  
					  t.[BRANDFAMILY_ID]= n.[BRANDFAMILY_ID] and
					  n.CAL_DATE > t.CAL_DATE and
					  n.CAL_DATE < dateadd(m,2,t.CAL_DATE)
		) a
		where pos= median_pos	
)


select  t.r R,
	t.CAL_DATE,p.CAL_DATE CAL_DATE_end,
	t.[CUSTOMER_ID],
	t.[BRANDFAMILY_ID],
	t.Midcategory,
	ceiling( t.SI_WSE) SI_WSE,
	max(case when ceiling( t.SI_WSE) = s.HI then SI_WSE_median else t.SI_WSE end) SI_WSE_ajust,
	max(s.SI_WSE_median) SI_WSE_median, 
	s.HI, 
	avg(s.std) std, avg(s.mean) mean
into [STAGING_2].[dbo].XXX_ITG_Sell_IN_Periods
from ITG_Sell_IN_top t
	join	ITG_Sell_IN_top p 
	on   t.[CUSTOMER_ID] = p.[CUSTOMER_ID] and
		 t.[BRANDFAMILY_ID] = p.[BRANDFAMILY_ID] and
		 p.r = t.r - 1
/*left join [STAGING_2].[dbo].XXX_ITG_Sell_IN_neg n 
	   on t.[CUSTOMER_ID]=n.[CUSTOMER_ID] and  
	      t.[BRANDFAMILY_ID]= n.[BRANDFAMILY_ID] and 
	      t.CAL_DATE < n.CAL_DATE  */
	      
left join ITG_Sell_IN_std  s
	   on --n.CAL_DATE is not null and 
		  t.[CUSTOMER_ID]=s.[CUSTOMER_ID] and  
	      t.[BRANDFAMILY_ID]= s.[BRANDFAMILY_ID] and
	      t.CAL_DATE between Date_Start and Date_End
group by t.r ,
	t.CAL_DATE,p.CAL_DATE,
	t.[CUSTOMER_ID],
	t.[BRANDFAMILY_ID],
	t.Midcategory,
	ceiling( t.SI_WSE) ,
	s.HI
	            
 order by 	t.CUSTOMER_ID,	t.BRANDFAMILY_ID, t.CAL_DATE

