/*with ITG_Sell_OUT as (
select	a16.[CAL_DATE] SO_DATE,
	a11.[CUSTOMER_ID]  CUSTOMER_ID,
	a13.[BRANDFAMILY_ID]  BRANDFAMILY_ID,
	a13.[SUBCATEGORY],
	a11.[Volumen] / a110.[CONVERSION_FACTOR]  SO_WSE,
	min(a16.[CAL_DATE]) over (partition by a11.[CUSTOMER_ID], a13.[BRANDFAMILY_ID] ) SO_Start,
	max(a16.[CAL_DATE]) over (partition by a11.[CUSTOMER_ID], a13.[BRANDFAMILY_ID] ) SO_End

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
)
*/
select	SI.r -1 R,
		SI.CAL_DATE,
		SI.CAL_DATE_end,
		SI.CUSTOMER_ID,
		SI.BRANDFAMILY_ID,
		SI.Midcategory,
--		SI.SI_WSE,
		SI.SI_ITG_WSE,
		ceiling(SUM(SO.SO_WSE)) SO_WSE
--		SI.SI_WSE_median,
--		SI.HI,
--		SI.std,
--		SI.mean
from [STAGING_2].[dbo].XXX_ITG_Sell_IN_Periods SI
left join [STAGING_2].[dbo].XXX_ITG_Sell_OUT SO 
	   on SI.CUSTOMER_ID 	= SO.CUSTOMER_ID and
		  SI.BRANDFAMILY_ID =	SO.BRANDFAMILY_ID and
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
		  
