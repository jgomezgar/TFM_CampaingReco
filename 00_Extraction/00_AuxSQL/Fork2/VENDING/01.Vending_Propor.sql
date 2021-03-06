
SELECT  a11.[CUSTOMER_ID]
	,case when a112.[Answer_Two] ='' then null else a112.[Answer_Two] end Sales2C
	,ceiling(isnull(case  a112.[Answer_Two] 
			  when '0% - 5%'   then  0.03
			  when '6% - 10%'  then  0.08
			  when '11% - 15%' then  0.13
			  when '16% - 20%' then  0.18
			  when '21% - 25%' then  0.23
			  when '26% - 30%' then  0.28
			  when '31% - 35%' then  0.33
			  when '36% - 40%' then  0.38
			  when '41% - 45%' then  0.43
			  when '46% - 50%' then  0.48
			  when '51% - 55%' then  0.53
			  when '56% - 60%' then  0.58
			  when '61% - 65%' then  0.63
			  when '66% - 70%' then  0.68
			  when '71% - 75%' then  0.73
			  when '76% - 80%' then  0.78
			  when '81% - 85%' then  0.83
			  when '86% - 90%' then  0.88
			  when '91% - 95%' then  0.93
			  when '96% - 100%' then 0.98
			  else 0 end,0) *100 )Sales2C_num
/*	,case when a112.[Answer_One] ='' then null else a112.[Answer_One] end tikets
	,case  a112.[Answer_One] 
	  when '<150' then  75           
	  when '>600' then  700
	  when 'Entre 151 y 250' then  200
	  when 'Entre 251 y 350' then  300
	  when 'Entre 351 y 400' then  375
	  when 'Entre 401 y 600' then  500
	ELSE 0 end tikets_num */
      , a13.[MES_ID]  MES
      , ISNULL(SUM(BLOND_VOL),0) [BLOND_VOL]   
      , ISNULL(SUM(BLOND_PCK),0) [BLOND_PCK]   
      , ISNULL(SUM(BLACK_VOL)/nullif(SUM(BLOND_VOL),0),0) [BLACK_VOL]   
      , ISNULL(SUM(BLACK_PCK)/nullif(SUM(BLOND_PCK),0),0) [BLACK_PCK]   
      , ISNULL(SUM(RYO_VOL)/nullif(SUM(BLOND_VOL),0),0) [RYO_VOL]       
      , ISNULL(SUM(RYO_PCK)/nullif(SUM(BLOND_PCK),0),0) [RYO_PCK]       
      , ISNULL(SUM(CIGAR_VOL)/nullif(SUM(BLOND_VOL),0),0) [CIGAR_VOL]   
      , ISNULL(SUM(PIPE_VOL)/nullif(SUM(BLOND_VOL),0),0) [PIPE_VOL]     
      , ISNULL(SUM(PIPE_PCK)/nullif(SUM(BLOND_PCK),0),0) [PIPE_PCK]     

  FROM [ITE_PRD].[ITE].[T_SMLM_CUSTTOTALS] a11  
  join ITE_PRD.ITE.LU_CLTE_1CANAL c on c.customer_id = a11.[Customer_ID]
	join	ITE.LU_MONTH	a12
	  on 	(a11.[SALESMONTH] = a12.[CAL_MONTH])
	join	ITE.V_Transf_MES_MQT1	a13
	  on 	(a12.[MES_ID] = a13.[MQT1])
left join	ITE.V_LU_CLTE_1CANAL_Assess	a112
	  on 	(a11.[CUSTOMER_ID] = a112.[CUSTOMER_ID] )	 
		and a112.[Nombre_Ficha] in (N'Datos Cuenta')
		and a112.[Assess_Status] in (N'Terminado')
        
-- los datos declarativos son recientes        
 where  a13.[MES_ID] between 201710 and 201910
 and not (c.NAME like'%C.P%' or name like'C.I.%' or name like'CI %' or  NAME like'%centro penit%')
  and [CITY] not in ('CEUTA', 'MELILLA') --and STATUS_CODE <> '92'
 -- and case when a112.[Answer_One] ='' then null else a112.[Answer_One] end is not null
group by  a11.[CUSTOMER_ID]
	,a13.[MES_ID]
	,a112.[Answer_Two]
--	,a112.[Answer_One]

