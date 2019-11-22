with median as(

select distinct * from (
select c.[CUSTOMER_ID], c.[BRANDFAMILY_ID], c.[Midcategory], 
  (DATEDIFF ( dd, CAL_DATE, CAL_DATE_end)) days_btwn_median,
  AVG(DATEDIFF ( dd, CAL_DATE, c.CAL_DATE_end)) over (partition by  c.[CUSTOMER_ID], c.[BRANDFAMILY_ID]) days_btwn_mean,
  c.r,
  pos = row_number() over (partition by c.[CUSTOMER_ID], c.[BRANDFAMILY_ID] order by DATEDIFF ( dd, CAL_DATE, c.CAL_DATE_end) desc ) ,
  round(0.51* MAX(c.r) over (partition by  c.[CUSTOMER_ID], c.[BRANDFAMILY_ID]),0) median_pos
from  [STAGING_2].[dbo].XXX_ITG_Sell_IN_Periods c
) a 
where pos= median_pos and median_pos>=3

)

, quality as (
select  a11.[Customer_ID], isnull(a12.[Muestra_so_ok],0) OK_13M, isnull(a13.[Muestra_so_ok],0) OK_15M
from  ITE.LU_CLTE_1CANAL  a11
  left join  ITE.V_Lu_Muestra_SO_1Canal_12M  a12
    on   (a11.[Customer_ID] = a12.[Customer_ID])
  left join  ITE.V_Lu_Muestra_SO_1Canal_15M  a13
    on   (a11.[Customer_ID] = a13.[Customer_ID])
)



SELECT
  C0.CUSTOMER_ID,
  OK_13M,
  OK_15M,
  C0.BRANDFAMILY_ID,
  C0.Midcategory,
  m.days_btwn_median,
  m.days_btwn_mean,
  
  C0.R R_0,
  C0.CAL_DATE CAL_DATE_0,
  C0.CAL_DATE_end CAL_DATE_end_0,
  C0.SI_ITG_WSE SI_ITG_WSE_0,
  C0.SI_MRKT_WSE SI_MRKT_WSE_0,
  C0.SO_ITG_WSE SO_ITG_WSE_0,
  C0.SO_MRKT_WSE SO_MRKT_WSE_0,
  C0.MECHERO MECHERO_0,
  C0.CLIPPER CLIPPER_0,
  C0.ABP ABP_0,
  C0.ABP_ESP ABP_ESP_0,
  C0.DISPENSADOR DISPENSADOR_0,
  C0.DISPENSADOR_ESP DISPENSADOR_ESP_0,
  C0.VISIBILIDAD VISIBILIDAD_0,
  C0.VISIBILIDAD_ESP VISIBILIDAD_ESP_0,
  C0.AZAFATA AZAFATA_0,
  C0.TOTEM TOTEM_0,
  C0.TOTEM_ESP TOTEM_ESP_0,
  C0.SVM SVM_0,
  C0.TFT TFT_0,
  C0.CUE CUE_0,
  
  C1.R R_1,
  C1.CAL_DATE CAL_DATE_1,
  C1.CAL_DATE_end CAL_DATE_end_1,
  C1.SI_ITG_WSE SI_ITG_WSE_1,
  C1.SI_MRKT_WSE SI_MRKT_WSE_1,
  C1.SO_ITG_WSE SO_ITG_WSE_1,
  C1.SO_MRKT_WSE SO_MRKT_WSE_1,
  C1.MECHERO MECHERO_1,
  C1.CLIPPER CLIPPER_1,
  C1.ABP ABP_1,
  C1.ABP_ESP ABP_ESP_1,
  C1.DISPENSADOR DISPENSADOR_1,
  C1.DISPENSADOR_ESP DISPENSADOR_ESP_1,
  C1.VISIBILIDAD VISIBILIDAD_1,
  C1.VISIBILIDAD_ESP VISIBILIDAD_ESP_1,
  C1.AZAFATA AZAFATA_1,
  C1.TOTEM TOTEM_1,
  C1.TOTEM_ESP TOTEM_ESP_1,
  C1.SVM SVM_1,
  C1.TFT TFT_1,
  C1.CUE CUE_1 ,
  
  C2.R R_2,
  C2.CAL_DATE CAL_DATE_2,
  C2.CAL_DATE_end CAL_DATE_end_2,
  C2.SI_ITG_WSE SI_ITG_WSE_2,
  C2.SI_MRKT_WSE SI_MRKT_WSE_2,
  C2.SO_ITG_WSE SO_ITG_WSE_2,
  C2.SO_MRKT_WSE SO_MRKT_WSE_2,
  C2.MECHERO MECHERO_2,
  C2.CLIPPER CLIPPER_2,
  C2.ABP ABP_2,
  C2.ABP_ESP ABP_ESP_2,
  C2.DISPENSADOR DISPENSADOR_2,
  C2.DISPENSADOR_ESP DISPENSADOR_ESP_2,
  C2.VISIBILIDAD VISIBILIDAD_2,
  C2.VISIBILIDAD_ESP VISIBILIDAD_ESP_2,
  C2.AZAFATA AZAFATA_2,
  C2.TOTEM TOTEM_2,
  C2.TOTEM_ESP TOTEM_ESP_2,
  C2.SVM SVM_2,
  C2.TFT TFT_2,
  C2.CUE CUE_2 /*,
  
  C3.R R_3,
  C3.CAL_DATE CAL_DATE_3,
  C3.CAL_DATE_end CAL_DATE_end_3,
  C3.SI_ITG_WSE SI_ITG_WSE_3,
  C3.SI_MRKT_WSE SI_MRKT_WSE_3,
  C3.SO_ITG_WSE SO_ITG_WSE_3,
  C3.SO_MRKT_WSE SO_MRKT_WSE_3,
  C3.MECHERO MECHERO_3,
  C3.CLIPPER CLIPPER_3,
  C3.ABP ABP_3,
  C3.ABP_ESP ABP_ESP_3,
  C3.DISPENSADOR DISPENSADOR_3,
  C3.DISPENSADOR_ESP DISPENSADOR_ESP_3,
  C3.VISIBILIDAD VISIBILIDAD_3,
  C3.VISIBILIDAD_ESP VISIBILIDAD_ESP_3,
  C3.AZAFATA AZAFATA_3,
  C3.TOTEM TOTEM_3,
  C3.TOTEM_ESP TOTEM_ESP_3,
  C3.SVM SVM_3,
  C3.TFT TFT_3,
  C3.CUE CUE_3,
  
  C4.R R_4,
  C4.CAL_DATE CAL_DATE_4,
  C4.CAL_DATE_end CAL_DATE_end_4,
  C4.SI_ITG_WSE SI_ITG_WSE_4,
  C4.SI_MRKT_WSE SI_MRKT_WSE_4,
  C4.SO_ITG_WSE SO_ITG_WSE_4,
  C4.SO_MRKT_WSE SO_MRKT_WSE_4,
  C4.MECHERO MECHERO_4,
  C4.CLIPPER CLIPPER_4,
  C4.ABP ABP_4,
  C4.ABP_ESP ABP_ESP_4,
  C4.DISPENSADOR DISPENSADOR_4,
  C4.DISPENSADOR_ESP DISPENSADOR_ESP_4,
  C4.VISIBILIDAD VISIBILIDAD_4,
  C4.VISIBILIDAD_ESP VISIBILIDAD_ESP_4,
  C4.AZAFATA AZAFATA_4,
  C4.TOTEM TOTEM_4,
  C4.TOTEM_ESP TOTEM_ESP_4,
  C4.SVM SVM_4,
  C4.TFT TFT_4,
  C4.CUE CUE_4,
  
  C5.R R_5,
  C5.CAL_DATE CAL_DATE_5,
  C5.CAL_DATE_end CAL_DATE_end_5,
  C5.SI_ITG_WSE SI_ITG_WSE_5,
  C5.SI_MRKT_WSE SI_MRKT_WSE_5,
  C5.SO_ITG_WSE SO_ITG_WSE_5,
  C5.SO_MRKT_WSE SO_MRKT_WSE_5,
  C5.MECHERO MECHERO_5,
  C5.CLIPPER CLIPPER_5,
  C5.ABP ABP_5,
  C5.ABP_ESP ABP_ESP_5,
  C5.DISPENSADOR DISPENSADOR_5,
  C5.DISPENSADOR_ESP DISPENSADOR_ESP_5,
  C5.VISIBILIDAD VISIBILIDAD_5,
  C5.VISIBILIDAD_ESP VISIBILIDAD_ESP_5,
  C5.AZAFATA AZAFATA_5,
  C5.TOTEM TOTEM_5,
  C5.TOTEM_ESP TOTEM_ESP_5,
  C5.SVM SVM_5,
  C5.TFT TFT_5,
  C5.CUE CUE_5,
  
  C6.R R_6,
  C6.CAL_DATE CAL_DATE_6,
  C6.CAL_DATE_end CAL_DATE_end_6,
  C6.SI_ITG_WSE SI_ITG_WSE_6,
  C6.SI_MRKT_WSE SI_MRKT_WSE_6,
  C6.SO_ITG_WSE SO_ITG_WSE_6,
  C6.SO_MRKT_WSE SO_MRKT_WSE_6,
  C6.MECHERO MECHERO_6,
  C6.CLIPPER CLIPPER_6,
  C6.ABP ABP_6,
  C6.ABP_ESP ABP_ESP_6,
  C6.DISPENSADOR DISPENSADOR_6,
  C6.DISPENSADOR_ESP DISPENSADOR_ESP_6,
  C6.VISIBILIDAD VISIBILIDAD_6,
  C6.VISIBILIDAD_ESP VISIBILIDAD_ESP_6,
  C6.AZAFATA AZAFATA_6,
  C6.TOTEM TOTEM_6,
  C6.TOTEM_ESP TOTEM_ESP_6,
  C6.SVM SVM_6,
  C6.TFT TFT_6,
  C6.CUE CUE_6 */

from [STAGING_2].[dbo].XXX_Sell_y_Activities C0

left join median m 
  on (C0.CUSTOMER_ID = m.CUSTOMER_ID  and
   C0.BRANDFAMILY_ID = m.BRANDFAMILY_ID ) 
left join quality q 
  on (C0.CUSTOMER_ID = q.CUSTOMER_ID)


join [STAGING_2].[dbo].XXX_Sell_y_Activities C1
  on (C0.CUSTOMER_ID = C1.CUSTOMER_ID  and
   C0.BRANDFAMILY_ID = C1.BRANDFAMILY_ID and
   C0.R = C1.R -1)

join [STAGING_2].[dbo].XXX_Sell_y_Activities C2
  on (C0.CUSTOMER_ID = C2.CUSTOMER_ID  and
   C0.BRANDFAMILY_ID = C2.BRANDFAMILY_ID and
   C0.R = C2.R -2)
/*   
join [STAGING_2].[dbo].XXX_Sell_y_Activities C3
  on (C0.CUSTOMER_ID = C3.CUSTOMER_ID  and
   C0.BRANDFAMILY_ID = C3.BRANDFAMILY_ID and
   C0.R = C3.R -3)
   
join [STAGING_2].[dbo].XXX_Sell_y_Activities C4
  on (C0.CUSTOMER_ID = C4.CUSTOMER_ID  and
   C0.BRANDFAMILY_ID = C4.BRANDFAMILY_ID and
   C0.R = C4.R -4)
   
join [STAGING_2].[dbo].XXX_Sell_y_Activities C5
  on (C0.CUSTOMER_ID = C5.CUSTOMER_ID  and
   C0.BRANDFAMILY_ID = C5.BRANDFAMILY_ID and
   C0.R = C5.R -5)
      
join [STAGING_2].[dbo].XXX_Sell_y_Activities C6
  on (C0.CUSTOMER_ID = C6.CUSTOMER_ID  and
   C0.BRANDFAMILY_ID = C6.BRANDFAMILY_ID and
   C0.R = C6.R -6)
   */
