
select	
    a11.Customer_ID,
    a11.[CITY]  Municipio,
	a11.[PROVINCE_TR_ID]  PROVINCE_TR_ID,
	a11.[POSTALCODE]  POSTALCODE0,
	a11.[DELIVERY_CODE]  DELIVERY_CODE,
	a12.[VISIT_FREQUENCY]  VISIT_FREQUENCY,
	a15.[PROVINCE_ID]  PROVINCE_ID,
	a17.[Siebel_Segment]  Siebel_Segment,
	a16.[RCT]  RCT,
	a12.[Status_Sbl]  Status,
	(a11.[Latitude])  Latitude,
	(a11.[Longitude])  Longitude
from	ITE.LU_CLTE_1CANAL	a11
	join	ITE.LU_CLTE_1C_M_SnpSht	a12
	  on 	(a11.[Customer_ID] = a12.[Customer_ID])
	join	ITE.T_PROVINCES_TR	a15
	  on 	(a11.[PROVINCE_TR_ID] = a15.[PROVINCE_TR_ID])
	join	ITE.T_PROVINCES	a16
	  on 	(a15.[PROVINCE_ID] = a16.[PROVINCE_ID])
	join	ITE.Lu_Siebel_Segment_TR	a17
	  on 	(a12.[Siebel_Segment] = a17.[Siebel_SubSegment])
	left outer join	ITE.v_LU_CONTACTS1C_UNIQUE_MAIL	a18
	  on 	(a11.[Customer_ID] = a18.[Customer_ID])
	left outer join	ITE.LU_VISIT_FREQUENCY	a19
	  on 	(a12.[VISIT_FREQUENCY] = a19.[VISIT_FREQUENCY])
group by	a11.[CITY]  ,
	a11.[PROVINCE_TR_ID]  ,
	a11.[POSTALCODE]  ,
	a11.[DELIVERY_CODE]  ,
	a12.[VISIT_FREQUENCY]  ,
	a15.[PROVINCE_ID]  ,
	a17.[Siebel_Segment]  ,
	a11.[Customer_ID]  ,
	(a11.[VAT_REGN])  ,
	(a11.[Latitude])  ,
	(a11.[Longitude])  ,
	a16.[RCT]  ,
	a12.[Status_Sbl]  