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

, invest_clean as (
select distinct
DIA, d.CAL_DATE,
/*	dia_inicio,
	case when Investment in ('MECHERO','ABP','CLIPPER') then 
			case when convert(int,convert(varchar(8), dateadd(dd, n_Item/10, CAL_DATE ),112)) > dia_fin then dia_fin else convert(int,convert(varchar(8), dateadd(dd, n_Item/10, CAL_DATE ),112)) end 
			
		 when Investment in ('Mueble', 'TFT','SVM') then convert(int,convert(varchar(8),GETDATE()-1 ,112))
		 else dia_fin end  dia_fin,*/
	Customer_ID,
	BRANDFAMILY_ID,
	Investment,
	1 n_Jornadas
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


--select distinct Investment from invet_clean --DIA,	Customer_ID,	BRANDFAMILY_ID,
, invest_column as (
select 	* from invest_clean
	Pivot ( sum(n_Jornadas) for 
		Investment in (MECHERO, CLIPPER, ABP, ABP_ESP, DISPENSADOR, DISPENSADOR_ESP, VISIBILIDAD, VISIBILIDAD_ESP ,AZAFATA, TOTEM, TOTEM_ESP, SVM, TFT, CUE))
	AS tablaPitot
)
--where 	MECHERO is not null


select 
  s.R,
  s.CAL_DATE,
  s.CAL_DATE_end,
  s.CUSTOMER_ID,
  s.BRANDFAMILY_ID,
  s.Midcategory,
  s.SI_ITG_WSE,
  s.SI_MRKT_WSE,
  s.SO_ITG_WSE,
  s.SO_MRKT_WSE,
  isnull(sum( MECHERO),0)        MECHERO,
  isnull(sum( CLIPPER),0)        CLIPPER,
  isnull(sum( ABP),0)            ABP,
  isnull(sum( ABP_ESP),0)        ABP_ESP,
  isnull(sum( DISPENSADOR),0)    DISPENSADOR,
  isnull(sum( DISPENSADOR_ESP),0)DISPENSADOR_ESP,
  isnull(sum( VISIBILIDAD),0)    VISIBILIDAD,
  isnull(sum( VISIBILIDAD_ESP),0)VISIBILIDAD_ESP,
  isnull(sum( AZAFATA),0)        AZAFATA,
  isnull(sum( TOTEM),0)          TOTEM,
  isnull(sum( TOTEM_ESP),0)      TOTEM_ESP,
  isnull(sum( SVM),0)            SVM,
  isnull(sum( TFT),0)            TFT,
  isnull(sum( CUE),0)            CUE    

from [STAGING_2].[dbo].XXX_Sell_Periods s 
left join invest_column i
  on s.CUSTOMER_ID = i.CUSTOMER_ID
  and s.BRANDFAMILY_ID = i.BRANDFAMILY_ID
  and i.CAL_DATE between s.CAL_DATE and s.CAL_DATE_end	
  
group by
  s.R,
  s.CAL_DATE,
  s.CAL_DATE_end,
  s.CUSTOMER_ID,
  s.BRANDFAMILY_ID,
  s.Midcategory,
  s.SI_ITG_WSE,
  s.SI_MRKT_WSE,
  s.SO_ITG_WSE,
  s.SO_MRKT_WSE


