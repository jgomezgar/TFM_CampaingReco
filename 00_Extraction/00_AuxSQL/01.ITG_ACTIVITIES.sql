with invest as (
select	
a15.[CAL_DATE]  CAL_DATE, 
dia_inicio , dia_fin,
a11.[Customer_ID]  Customer_ID,
a13.[BRANDFAMILY_ID]  BRANDFAMILY_ID,
--	a11.[Investment_type]  Investment_type,
	UPPER(
	case when CHARINDEX('vending', item) > 0  then 'SVM' 
		when Item_type = 'abp' OR (CHARINDEX('ABP', a11.Investment_type) > 0  and Item_type not in ('mechero','clipper')) then 'ABP' 
		when a11.Investment_type IN ('Visibilidad Temporal','Visibilidad Permanente') and Item_type not in ( 'dispensador','totem','tft')  then 'Visibilidad'
		when a11.Investment_type IN ('CUE')  then a11.Investment_type
		else  Item_type end
		+
	max(case when a12.[Concepto] <> 'BAU' then '_ESP' else '' end)
	) Investment,
--a12.[Concepto],
	sum(case when a11.[n_Item] < 300 then a11.[n_Item] else 300 end )  [n_Item],
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
	  on 	(a15.[DIA] = dia_inicio )--and dia_fin)	
	join	ITE.V_LU_BRANDFAMILIES	a18
	  on 	(a13.[BRANDFAMILY_ID] = a18.[BRANDFAMILY_ID])
	  
	  
	  
where	--a11.Dia >= 20161000--
--a15.CAL_MONTH  >= '201710'
   a14.[Midcategory] in (N'BLOND', N'RYO') and a11.Customer_ID is not null
 and n_Item > 0
 
group by	
	a15.[CAL_DATE], 
	dia_inicio , dia_fin,
	a11.[Customer_ID],
	a13.[BRANDFAMILY_ID],
		case when CHARINDEX('vending', item) > 0  then 'SVM' 
			when Item_type = 'abp' OR (CHARINDEX('ABP', a11.Investment_type) > 0  and Item_type not in ('mechero','clipper')) then 'ABP' 
			when a11.Investment_type IN ('Visibilidad Temporal','Visibilidad Permanente') and Item_type not in ( 'dispensador','totem','tft')  then 'Visibilidad'
			when a11.Investment_type IN ('CUE')  then a11.Investment_type
			else  Item_type end
			



		
	
--a12.[Concepto]
)

, invet_clean as (
select 
DIA,
/*	dia_inicio,
	case when Investment in ('MECHERO','ABP','CLIPPER') then 
			case when convert(int,convert(varchar(8), dateadd(dd, n_Item/10, CAL_DATE ),112)) > dia_fin then dia_fin else convert(int,convert(varchar(8), dateadd(dd, n_Item/10, CAL_DATE ),112)) end 
			
		 when Investment in ('Mueble', 'TFT','SVM') then convert(int,convert(varchar(8),GETDATE()-1 ,112))
		 else dia_fin end  dia_fin,*/
	Customer_ID,
	BRANDFAMILY_ID,
	Investment,
	1 n_Dias
	--coste 
 from invest i
 join	ITE.T_DAY	d
   on (d.[DIA] between  dia_inicio  and 
			case when Investment in ('MECHERO','ABP','CLIPPER') then 
					case when convert(int,convert(varchar(8), dateadd(dd, n_Item/10, i.CAL_DATE ),112)) > dia_fin then dia_fin else convert(int,convert(varchar(8), dateadd(dd, n_Item/10, i.CAL_DATE ),112)) end 
				 when Investment in ('TFT','SVM') then convert(int,convert(varchar(8),GETDATE()-1 ,112))
			 else dia_fin end )
where d.CAL_MONTH  >= '201710'	 
) 	


--select distinct Investment from invet_clean

--select DIA,	Customer_ID,	BRANDFAMILY_ID,	ABP, ABP_ESP, CUE, SVM, TFT, VISIBILIDAD, TOTEM, VISIBILIDAD_ESP, DISPENSADOR_ESP, CLIPPER, AZAFATA, DISPENSADOR, TOTEM_ESP, MECHERO
--order by 2,3,1
