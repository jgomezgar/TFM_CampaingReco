/*********** 19   min *****************/
/*  RESULT: Datos distribucion Vending
   [STAGING_2].[dbo].XXX_Vending_Sell  
*/

/*#########################################################################*/
IF OBJECT_ID('[STAGING_2].[dbo].XXX_Vending_Sell', 'U') IS NOT NULL
 DROP TABLE [STAGING_2].[dbo].XXX_Vending_Sell;


select	a11.[MES]  MES,
	a16.[PROVINCE_ID]  PROVINCE_ID,
	a16.CP,
	a15.[CUSTOMER_ID]  CUSTOMER_ID,
	a13.[Subcategory]  Subcategory,
--	case when a17.[COMPANY_ID] in ('CC000003', 'CC000040', 'CC000070', 'CC000074', 'CC000169') then 'ITG' else 'Competence' end [COMPANY],
	a13.[BRANDFAMILY_ID]  BRANDFAMILY_ID,
	sum(a11.[Ventas_vol])  [ITG_SV_vol],
	sum(sum(a11.[Ventas_vol])) over (partition by a11.[MES], a15.[CUSTOMER_ID],a13.[Subcategory]) [Mrkt_SV_vol]
into [STAGING_2].[dbo].XXX_Vending_Sell
from	ITE.FACT_SELLOUT_2C_Ventas	a11
	join	ITE.T_PRODUCTS	a12
	  on 	(a11.[PRODUCT_ID] = a12.[PRODUCT_ID])
	join	ITE.T_BRANDPACKS	a13
	  on 	(a12.[BRANDPACK_ID] = a13.[BRANDPACK_ID])
	join	ITE.V_LU_OPERADORES_SELLOUT_2C	a14
	  on 	(a11.[IdMaquina] = a14.[IdMaquina])
	join	ITE.v_REL_OPER_CUSTOMER	a15
	  on 	(a14.[IdEmpresa] = a15.[OPERADOR_ID])
	join	ITE.V_LU_MAQUINAS_SELLOUT_2C	a16
	  on 	(a11.[IdMaquina] = a16.[IdMaquina])
--	join	ITE.V_LU_BRANDFAMILIES	a17
--	  on 	(a13.[BRANDFAMILY_ID] = a17.[BRANDFAMILY_ID])
--	join	ITE.T_PROVINCES	a19
--	  on 	(a16.[PROVINCE_ID] = a19.[PROVINCE_ID])
--	join	ITE.LU_MONTH	a110
--	  on 	(a11.[MES] = a110.[MES_id])
where	(a11.[MES] between '201600' and '201910'
 and a13.[Subcategory] in (N'Blond'))
group by	a11.[MES],
	a16.[PROVINCE_ID],
	a16.CP,
	a15.[CUSTOMER_ID],
	a13.[Subcategory],
	a13.[BRANDFAMILY_ID]
--	case when a17.[COMPANY_ID] in ('CC000003', 'CC000040', 'CC000070', 'CC000074', 'CC000169') then 'ITG' else 'Competence' end 

