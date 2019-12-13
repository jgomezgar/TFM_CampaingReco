
with multimarcaList as (
select 	0 Zona_fortuna_id,'DOM' Siebel_Segment, 'BF234103' BrandFamily_id union all 
select 0, 'DOM',	'BF241049' union all 
select  0, 'DOM',	'BF241151' union all 
select	1 Zona_fortuna_id,'DOM' Siebel_Segment, 'BF234103' BrandFamily_id union all 
select 1, 'DOM',	'BF241049' union all 
select  1, 'DOM',	'BF241151' union all 
select  0 ,'DOM', 'BF234104' union all 
select 1 ,'DOM',	'BF231021' union all
select 0, 'TR 2 BRITANICO', 'BF141002' union all
select 0, 'TR 2 BRITANICO', 'BF132002' union all
select 0, 'TR 2 BRITANICO', 'BF132013' union all
select 0, 'TR 1 FRANCES', 'BF131001' union all
select 0, 'TR 1 FRANCES', 'BF141038' union all
select 0, 'TR 2 BRITANICO', 'BF132005' union all
select 0, 'TR 1 FRANCES', 'BF231084' union all
select 0, 'TR 1 FRANCES', 'BF241061' union all
select 0, 'TR 2 BRITANICO', 'BF234110' union all
select 0, 'TR 2 ALEMAN', 'BF131007' union all
select 0, 'TR 2 MIXTO', 'BF141002' union all
select 0, 'TR 2 MIXTO', 'BF132002' union all
select 0, 'TR 2 MIXTO', 'BF132013' union all
select 0, 'TR 2 MIXTO', 'BF132005' union all
select 0, 'TR 2 MIXTO', 'BF234110' union all
select 1, 'TR 2 BRITANICO', 'BF141002' union all
select 1, 'TR 2 BRITANICO', 'BF132002' union all
select 1, 'TR 2 BRITANICO', 'BF132013' union all
select 1, 'TR 1 FRANCES', 'BF131001' union all
select 1, 'TR 1 FRANCES', 'BF141038' union all
select 1, 'TR 2 BRITANICO', 'BF132005' union all
select 1, 'TR 1 FRANCES', 'BF231084' union all
select 1, 'TR 1 FRANCES', 'BF241061' union all
select 1, 'TR 2 BRITANICO', 'BF234110' union all
select 1, 'TR 2 ALEMAN', 'BF131007' union all
select 1, 'TR 2 MIXTO', 'BF141002' union all
select 1, 'TR 2 MIXTO', 'BF132002' union all
select 1, 'TR 2 MIXTO', 'BF132013' union all
select 1, 'TR 2 MIXTO', 'BF132005' union all
select 1, 'TR 2 MIXTO', 'BF234110' )

, MM as (
select distinct  'BF99999' multi, 4 plex,	a11.[Customer_ID]  Customer_ID, m.BrandFamily_id
from	ITE.LU_CLTE_1CANAL	a11
	join	ITE.T_PROVINCES_TR	a12
	  on 	(a11.[PROVINCE_TR_ID] = a12.[PROVINCE_TR_ID])
	join	ITE.T_PROVINCES	a13
	  on 	(a12.[PROVINCE_ID] = a13.[PROVINCE_ID])
	--join	ITE.LU_SEGM_TURISMO	a14
	--  on 	(Case when substring(a11.[Siebel_Segment],1,2) = 'DO' THEN 'Doméstico' WHEN SUBSTRING(a11.[Siebel_Segment],1,2) = 'TR' THEN 'Travel Retail' ELSE 'ND' END = Case when substring(a14.[SEGM_TURISM_ID],1,2) = 'DO' THEN 'Doméstico' WHEN SUBSTRING(a14.[SEGM_TURISM_ID],1,2) = 'TR' THEN 'Travel Retail' ELSE 'ND' END and 
	--	a11.[Siebel_Segment] = a14.[SEGM_TURISM_ID])
	--ABP packs, Dispensadores item * 40 resto distinc_items
	
	join multimarcaList m
		on m.Zona_fortuna_id = a13.[ZONA_FORTUNA] and
		   m.Siebel_Segment = a11.Siebel_Segment
	   
) 

select	
a15.[CAL_DATE]  CAL_DATE, 
dia_inicio , dia_fin,
a11.[Customer_ID]  Customer_ID,
case when a13.[BRANDFAMILY_ID] in ('BF999999','BF999998') then MM.BrandFamily_id else a13.BRANDFAMILY_ID end BRANDFAMILY_ID,
--	a11.[Investment_type]  Investment_type,
--	Item_type,
--	a12.[Concepto],
--	a11.Origen,
	UPPER(
	case when CHARINDEX('vending', item) > 0  then 'SVM' 
		when Item_type = 'abp' OR (CHARINDEX('ABP', a11.Investment_type) > 0  and Item_type not in ('mechero','clipper')) then 'ABP' 
		when a11.Investment_type IN ('Visibilidad Temporal','Visibilidad Permanente','Visibilidad Ciclo') and Item_type not in ( 'dispensador','totem','tft')  then 'Visibilidad'
		when a11.Investment_type IN ('CUE')  then a11.Investment_type
		else  Item_type end
		+
	case when CHARINDEX('ABP', a11.Investment_type) = 0  and a12.[Concepto] <> 'BAU' then '_ESP' else '' end
	) Investment,
	
    /*ceiling(
    case when max(CHARINDEX('ABP', a11.Investment_type)) > 0   OR max(Item_type)  in ('mechero','clipper','abp') then   
			sum(case when a11.Origen = 'Logista' OR a11.[n_packs] < 200 then a11.[n_packs] else 200  end ) 
		else case when max(Item_type) in ( 'dispensador') then sum(case when a11.Origen = 'Logista' OR a11.[n_Item] < 5 then a11.[n_Item] else 5  end) *40  
				 else count(distinct Item_type) end 
		end ) [n_Item],*/
	sum(case when a11.Origen = 'Logista' OR a11.[n_packs] < 200 then a11.[n_packs] else 200  end ) 	[n_packs],
	sum(case when a11.Origen = 'Logista' OR a11.[n_Item] < 5 then a11.[n_Item] else 5  end) [n_Item],
	count(distinct Item_type) distinct_items,
	sum(a11.[coste]) /ISNULL(max(plex),1)   [coste]
	
	
--	count(distinct Item_type) count_distint_item,
--	SUM([n_Item]) sum_num_ITEM,
--	SUM([n_packs]) sum_num_paks,
--	sum(a11.[coste])
	
from	ITE.Fact_Invest_Actuals_Daily	a11
	join	ITE.LU_Invest_Items	a12
	  on 	(a11.[Investment_type] = a12.[Investment_type] and 
	a11.[Item_id] = a12.[Item_id])
	join	ITE.T_BRANDPACKS	a13
	  on 	(a11.[BRANDPACK_ID] = a13.[BRANDPACK_ID])
--	join	ITE.LU_SUBCATEGORY	a14
--	  on 	(a13.[SUBCATEGORY] = a14.[SUBCATEGORY])
	join	ITE.T_DAY	a15
	  on 	(a15.[DIA] = dia_inicio )--and dia_fin)	
--	join	ITE.V_LU_BRANDFAMILIES	a18
--	  on 	(a13.[BRANDFAMILY_ID] = a18.[BRANDFAMILY_ID])
	left join MM on a11.CUSTOMER_ID = MM.Customer_ID  and 
	CHARINDEX(MM.multi, a13.[BRANDFAMILY_ID]) > 0	  
			
	  
	  
where	--a11.Dia >= 20161000--
a15.CAL_MONTH  >= '201710' and
   a13.SUBCATEGORY in (N'BLOND', N'RYO') and a11.Customer_ID is not null
 and n_Item > 0
 
group by	

a15.[CAL_DATE], 
dia_inicio , dia_fin,
a11.[Customer_ID],
case when a13.[BRANDFAMILY_ID] in ('BF999999','BF999998') then MM.BrandFamily_id else a13.BRANDFAMILY_ID end,
--	a11.[Investment_type]  Investment_type,
--	Item_type,
--	a12.[Concepto],
--	a11.Origen,
	UPPER(
	case when CHARINDEX('vending', item) > 0  then 'SVM' 
		when Item_type = 'abp' OR (CHARINDEX('ABP', a11.Investment_type) > 0  and Item_type not in ('mechero','clipper')) then 'ABP' 
		when a11.Investment_type IN ('Visibilidad Temporal','Visibilidad Permanente','Visibilidad Ciclo') and Item_type not in ( 'dispensador','totem','tft')  then 'Visibilidad'
		when a11.Investment_type IN ('CUE')  then a11.Investment_type
		else  Item_type end
		+
	case when CHARINDEX('ABP', a11.Investment_type) = 0  and a12.[Concepto] <> 'BAU' then '_ESP' else '' end
	) 
order by 1,4,5