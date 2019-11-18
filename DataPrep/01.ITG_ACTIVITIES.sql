select	
a15.[CAL_DATE]  CAL_DATE,
a11.[Customer_ID]  Customer_ID,
a13.[BRANDFAMILY_ID]  BRANDFAMILY_ID,
	a11.[Investment_type]  Investment_type,
	a12.[Item_type]  Item_type,
	a12.[Concepto]  Concepto,

	sum(a11.[n_Item])  [n_Item],
	sum(a11.[coste])  [coste]
from	ITE.Fact_Invest_Actuals_Daily	a11
	join	ITE.LU_Invest_Items	a12
	  on 	(a11.[Investment_type] = a12.[Investment_type] and 
	a11.[Item_id] = a12.[Item_id])
	join	ITE.T_BRANDPACKS	a13
	  on 	(a11.[BRANDPACK_ID] = a13.[BRANDPACK_ID])
	join	ITE.LU_SUBCATEGORY	a14
	  on 	(a13.[SUBCATEGORY] = a14.[SUBCATEGORY])
	join	ITE.T_DAY	a15
	  on 	(a15.[DIA] between dia_inicio and dia_fin)
	
	
	join	ITE.V_LU_BRANDFAMILIES	a18
	  on 	(a13.[BRANDFAMILY_ID] = a18.[BRANDFAMILY_ID])
where	a15.CAL_MONTH  >= '201601'
 and  a14.[Midcategory] in (N'BLOND', N'RYO')
group by	a15.[CAL_DATE]  ,
a11.[Customer_ID]  ,
a13.[BRANDFAMILY_ID]  ,
	a11.[Investment_type],
	a12.[Item_type],
	a12.[Concepto] 
	
	order by 2,3,1