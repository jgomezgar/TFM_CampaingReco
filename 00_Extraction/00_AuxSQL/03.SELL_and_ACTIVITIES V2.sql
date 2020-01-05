/*################### 3 min #################################################*/


/*#########################################################################*/
IF OBJECT_ID('[STAGING_2].[dbo].XXX_Sell_y_Activities_10d', 'U') IS NOT NULL
 DROP TABLE [STAGING_2].[dbo].XXX_Sell_y_Activities_10d;

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

,invest as (
select	
a15.[CAL_DATE]  CAL_DATE, 
dia_inicio , 
--case when a12.Item_type in ('dispensador') then  convert(int,convert(varchar,dateadd(dd,3,a15.[CAL_DATE]),112)) else  dia_fin end 
dia_fin,
a11.[Customer_ID]  Customer_ID,
case when a13.[BRANDFAMILY_ID] in ('BF999999','BF999998') then MM.BrandFamily_id else a13.BRANDFAMILY_ID end BRANDFAMILY_ID,

	UPPER(
	case 
		when (a11.Investment_type = 'ABP' OR Item_type='case') and Item_type not in ('mechero','clipper') then 'ABP' 
		when a11.Investment_type IN ('display - luminoso','Visibilidad Permanente','Visibilidad Ciclo') and Item_type not in ( 'dispensador','totem','tft','SVM')  then 'Visibilidad'
		when a11.Investment_type IN ('CUE')  then a11.Investment_type
		else  Item_type end
		+
	case when(a11.Investment_type <> 'ABP' OR Item_type not in ( 'dispensador')) and a12.[Concepto] <> 'BAU' then '_ESP' else '' end	) Investment,	
   
   
	sum(case when a11.Origen = 'Logista' OR a11.[n_packs] < 200 then isnull(nullif(a11.[n_packs],0), a11.[n_Item])
	         when Item_type in ('dispensador') and (a11.Origen = 'Logista' OR a11.[n_Item] < 5) then a11.[n_Item] * 40 
		else 200  end ) 	[n_packs],
	sum(case when a11.Origen = 'Logista' OR a11.[n_Item] < 3 then a11.[n_Item] else 3  end) [n_Item],
	count(distinct Item_type) distinct_items,
	sum(a11.[coste]) /ISNULL(max(plex),1)   [coste]
	
	/*,
	
	
	count(distinct Item_type) count_distint_item,
	SUM([n_Item]) sum_num_ITEM,
	SUM([n_packs]) sum_num_paks,
	sum(a11.[coste]) */
	
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
a15.CAL_MONTH  >= '201609' and
   a13.SUBCATEGORY in (N'BLOND', N'RYO') and a11.Customer_ID is not null
 --and n_Item > 0
 
group by	

a15.[CAL_DATE], 
dia_inicio , 
dia_fin,
-- case when a12.Item_type in ('dispensador') then  convert(int,convert(varchar,dateadd(dd,3,a15.[CAL_DATE]),112)) else dia_fin end,
a11.[Customer_ID],
case when a13.[BRANDFAMILY_ID] in ('BF999999','BF999998') then MM.BrandFamily_id else a13.BRANDFAMILY_ID end,
--	a11.[Investment_type]  Investment_type,
--	Item_type,
--	a12.[Concepto],
--	a11.Origen,
	UPPER(
	case 
		when (a11.Investment_type = 'ABP' OR Item_type='case') and Item_type not in ('mechero','clipper') then 'ABP' 
		when a11.Investment_type IN ('display - luminoso','Visibilidad Permanente','Visibilidad Ciclo') and Item_type not in ( 'dispensador','totem','tft','SVM')  then 'Visibilidad'
		when a11.Investment_type IN ('CUE')  then a11.Investment_type
		else  Item_type end
		+
	case when (a11.Investment_type <> 'ABP' OR Item_type not in ( 'dispensador')) AND a12.[Concepto] <> 'BAU' then '_ESP' else '' end
	)	
	
--having count(distinct Item_type) > 3 
)

, invest_clean as (
select 
DIA, d.CAL_DATE,
/*	dia_inicio,
	case when Investment in ('MECHERO','ABP','CLIPPER') then 
			case when convert(int,convert(varchar(8), dateadd(dd, n_Item/10, CAL_DATE ),112)) > dia_fin then dia_fin else convert(int,convert(varchar(8), dateadd(dd, n_Item/10, CAL_DATE ),112)) end 
			
		 when Investment in ('Mueble', 'TFT','SVM') then convert(int,convert(varchar(8),GETDATE()-1 ,112))
		 else dia_fin end  dia_fin,*/
	Customer_ID,
	BRANDFAMILY_ID,
	Investment,
	case when Investment  in ('TFT','SVM', 'VISIBILIDAD') then distinct_items else 1 end Intensidad
	
	   /*ceiling(
    case when max(CHARINDEX('ABP', a11.Investment_type)) > 0   OR max(Item_type)  in ('mechero','clipper','abp') then   
			sum(case when a11.Origen = 'Logista' OR a11.[n_packs] < 200 then a11.[n_packs] else 200  end ) 
		else case when max(Item_type) in ( 'dispensador') then sum(case when a11.Origen = 'Logista' OR a11.[n_Item] < 5 then a11.[n_Item] else 5  end) *40  
				 else count(distinct Item_type) end 
		end ) [n_Item],*/
	
	
	--coste 
 from invest i
 join	ITE.T_DAY	d
   on (d.[DIA] between  dia_inicio  and 
			case when Investment in ('MECHERO','ABP','CLIPPER','DISPENSADOR') then 
					case when convert(int,convert(varchar(8), dateadd(dd, [n_packs]/10, i.CAL_DATE ),112)) > dia_fin then dia_fin else convert(int,convert(varchar(8), dateadd(dd, [n_packs]/10, i.CAL_DATE ),112)) end 
				 when Investment in ('TFT','SVM') then convert(int,convert(varchar(8),GETDATE()-1 ,112))
			 else dia_fin end )
where d.CAL_MONTH  >= '201710'	and d.CAL_MONTH < convert(varchar(6),GETDATE()-1,112)

) 	


--select distinct Investment from invet_clean --DIA,	Customer_ID,	BRANDFAMILY_ID,
, invest_column as (
select 	* from invest_clean
	Pivot ( sum(Intensidad) for 
		Investment in (MECHERO, CLIPPER, ABP,DISPENSADOR, VISIBILIDAD, VISIBILIDAD_ESP ,AZAFATA, TOTEM, TOTEM_ESP, SVM, TFT, CUE))
	AS tablaPitot
)
--where 	TFT > 1-- is not null

,visits as (
select	
	d.[CAL_DATE]  ,
--	a11.[ROL]  ROL,
--	a12.[POSICION_DESC],
	a11.[Customer_ID],
	1  visit
from	ITE.v_FACT_VISITS	a11
	join	ITE.LU_FUERZA_DE_VENTAS	a12
	  on 	(a11.[EMP_ID] = a12.[EMP_ID])
	join	ITE.T_DAY	d
	  on 	(a11.[DIA] = d.[DIA])
where	(a11.[DIA] between 20171000 and convert(int, convert(varchar(8),GETDATE()-1,112))
 and a12.[POSICION_DESC] not in (N'Blu'))
group by		d.[CAL_DATE]  ,
--	a11.[ROL] ,
--	a12.[POSICION_DESC],
	a11.[Customer_ID]
)


, Sell_Periods_10d_rich_dates as (
select 
	p.R,
	p.tercio,
	p.NUM_SELLING_DAYS,
	p.NUM_DAYS,
	p.days_btw_order,
	COUNT(distinct s.cal_date) num_orders,
	p.CAL_DATE,
	p.CAL_DATE_end,
	p.CUSTOMER_ID,
	p.BRANDFAMILY_ID,
	p.Midcategory,
	p.SI_ITG_WSE,
	p.SI_MRKT_WSE,
	p.SO_ITG_WSE,
	p.SO_MRKT_WSE
from [STAGING_2].[dbo].XXX_Sell_Periods_10d p
left join [STAGING_2].[dbo].XXX_ITG_Sell_IN s
		on s.cal_date between p.CAL_DATE and p.CAL_DATE_end
		and p.CUSTOMER_ID = s.CUSTOMER_ID
		and p.BRANDFAMILY_ID = s.BRANDFAMILY_ID
	
group by 
	p.R,
	p.tercio,
	p.NUM_SELLING_DAYS,
	p.NUM_DAYS,
	p.days_btw_order,
	p.CAL_DATE,
	p.CAL_DATE_end,
	p.CUSTOMER_ID,
	p.BRANDFAMILY_ID,
	p.Midcategory,
	p.SI_ITG_WSE,
	p.SI_MRKT_WSE,
	p.SO_ITG_WSE,
	p.SO_MRKT_WSE
)



select 
  s.R,
	s.tercio,
	s.NUM_SELLING_DAYS,
	s.NUM_DAYS,
	s.days_btw_order,
	s.num_orders,
  s.CAL_DATE,
  s.CAL_DATE_end,
  s.CUSTOMER_ID,
  s.BRANDFAMILY_ID,
  s.Midcategory,
  isnull(s.SI_ITG_WSE,0) SI_ITG_WSE,
  isnull(s.SI_MRKT_WSE,0) SI_MRKT_WSE,
  isnull(s.SO_ITG_WSE,0) SO_ITG_WSE,
  isnull(s.SO_MRKT_WSE,0) SO_MRKT_WSE,
  isnull(isnull(s.SI_ITG_WSE,0) / nullif(s.SI_MRKT_WSE,0),0) QUOTA_SELLIN,
  isnull(isnull(s.SO_ITG_WSE,0) / nullif(s.SO_MRKT_WSE,0),0) QUOTA_SELLOUT,
  sum( isnull(MECHERO,0))        MECHERO,
  sum( isnull(CLIPPER,0))        CLIPPER,
  sum( isnull(ABP,0))            ABP,
  sum( isnull(DISPENSADOR,0))    DISPENSADOR,
  sum( isnull(VISIBILIDAD,0))    VISIBILIDAD,
  sum( isnull(VISIBILIDAD_ESP,0))VISIBILIDAD_ESP,
  sum( isnull(AZAFATA,0))        AZAFATA,
  sum( isnull(TOTEM,0))          TOTEM,
  sum( isnull(TOTEM_ESP,0))      TOTEM_ESP,
  sum( isnull(SVM,0))            SVM,
  sum( isnull(TFT,0))            TFT,
  sum( isnull(CUE,0))            CUE,
  sum( isnull(visit,0))          visit      
into [STAGING_2].[dbo].XXX_Sell_y_Activities_10d 
from Sell_Periods_10d_rich_dates s 
left join invest_column i
  on s.CUSTOMER_ID = i.CUSTOMER_ID
  and s.BRANDFAMILY_ID = i.BRANDFAMILY_ID
  and i.CAL_DATE between s.CAL_DATE and s.CAL_DATE_end	
left join visits v  
  on s.CUSTOMER_ID = v.CUSTOMER_ID 
  and v.CAL_DATE between s.CAL_DATE and s.CAL_DATE_end	
 where  S.CAL_DATE  >= '2017-10-01'	 
group by
  s.R,
	s.tercio,
	s.NUM_SELLING_DAYS,
	s.NUM_DAYS,
	s.days_btw_order,
	s.num_orders,
  s.CAL_DATE,
  s.CAL_DATE_end,
  s.CUSTOMER_ID,
  s.BRANDFAMILY_ID,
  s.Midcategory,
  s.SI_ITG_WSE,
  s.SI_MRKT_WSE,
  s.SO_ITG_WSE,
  s.SO_MRKT_WSE