/*################### 10 min #################################################*/


/*#########################################################################*/
IF OBJECT_ID('[STAGING_2].[dbo].XXX_Sell_y_Activities', 'U') IS NOT NULL
 DROP TABLE [STAGING_2].[dbo].XXX_Sell_y_Activities;

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
select distinct 	a11.[Customer_ID]  Customer_ID, m.BrandFamily_id
from	ITE.LU_CLTE_1CANAL	a11
	join	ITE.T_PROVINCES_TR	a12
	  on 	(a11.[PROVINCE_TR_ID] = a12.[PROVINCE_TR_ID])
	join	ITE.T_PROVINCES	a13
	  on 	(a12.[PROVINCE_ID] = a13.[PROVINCE_ID])
	--join	ITE.LU_SEGM_TURISMO	a14
	--  on 	(Case when substring(a11.[Siebel_Segment],1,2) = 'DO' THEN 'Doméstico' WHEN SUBSTRING(a11.[Siebel_Segment],1,2) = 'TR' THEN 'Travel Retail' ELSE 'ND' END = Case when substring(a14.[SEGM_TURISM_ID],1,2) = 'DO' THEN 'Doméstico' WHEN SUBSTRING(a14.[SEGM_TURISM_ID],1,2) = 'TR' THEN 'Travel Retail' ELSE 'ND' END and 
	--	a11.[Siebel_Segment] = a14.[SEGM_TURISM_ID])
	join multimarcaList m
		on m.Zona_fortuna_id = a13.[ZONA_FORTUNA] and
		   m.Siebel_Segment = a11.Siebel_Segment
	   
) 

,invest as (
select	
a15.[CAL_DATE]  CAL_DATE, 
dia_inicio , dia_fin,
a11.[Customer_ID]  Customer_ID,
case when a13.[BRANDFAMILY_ID] in ('BF999999','BF999998') then MM.BrandFamily_id else a13.BRANDFAMILY_ID end BRANDFAMILY_ID,
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
--	join	ITE.LU_SUBCATEGORY	a14
--	  on 	(a13.[SUBCATEGORY] = a14.[SUBCATEGORY])
	join	ITE.T_DAY	a15
	  on 	(a15.[DIA] = dia_inicio )--and dia_fin)	
	join	ITE.V_LU_BRANDFAMILIES	a18
	  on 	(a13.[BRANDFAMILY_ID] = a18.[BRANDFAMILY_ID])
	join MM on a11.CUSTOMER_ID = MM.Customer_ID  
	  
			
	  
	  
where	--a11.Dia >= 20161000--
--a15.CAL_MONTH  >= '201710'
   a13.SUBCATEGORY in (N'BLOND', N'RYO') and a11.Customer_ID is not null
 and n_Item > 0
 
group by	
	a15.[CAL_DATE], 
	dia_inicio , dia_fin,
	a11.[Customer_ID],
	case when a13.[BRANDFAMILY_ID] in ('BF999999','BF999998') then MM.BrandFamily_id else a13.BRANDFAMILY_ID end ,
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
  isnull(s.SI_ITG_WSE,0) SI_ITG_WSE,
  isnull(s.SI_MRKT_WSE,0) SI_MRKT_WSE,
  isnull(s.SO_ITG_WSE,0) SO_ITG_WSE,
  isnull(s.SO_MRKT_WSE,0) SO_MRKT_WSE,
  sum( isnull(MECHERO,0))        MECHERO,
  sum( isnull(CLIPPER,0))        CLIPPER,
  sum( isnull(ABP,0))            ABP,
  sum( isnull(ABP_ESP,0))        ABP_ESP,
  sum( isnull(DISPENSADOR,0))    DISPENSADOR,
  sum( isnull(DISPENSADOR_ESP,0))DISPENSADOR_ESP,
  sum( isnull(VISIBILIDAD,0))    VISIBILIDAD,
  sum( isnull(VISIBILIDAD_ESP,0))VISIBILIDAD_ESP,
  sum( isnull(AZAFATA,0))        AZAFATA,
  sum( isnull(TOTEM,0))          TOTEM,
  sum( isnull(TOTEM_ESP,0))      TOTEM_ESP,
  sum( isnull(SVM,0))            SVM,
  sum( isnull(TFT,0))            TFT,
  sum( isnull(CUE,0))            CUE    
into [STAGING_2].[dbo].XXX_Sell_y_Activities 
from [STAGING_2].[dbo].XXX_Sell_Periods s 
left join invest_column i
  on s.CUSTOMER_ID = i.CUSTOMER_ID
  and s.BRANDFAMILY_ID = i.BRANDFAMILY_ID
  and i.CAL_DATE between s.CAL_DATE and s.CAL_DATE_end	
 where  S.CAL_DATE  >= '2017-10-01'	 
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


