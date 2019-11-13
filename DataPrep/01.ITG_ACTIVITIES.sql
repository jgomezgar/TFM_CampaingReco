select    a11.[Customer_ID]  Customer_ID,
                max(a16.[NAME])  Customer_NAME,
                a11.[Investment_type]  Investment_type,
                a11.[dia_inicio]  start_day,
                a11.[dia_fin]  end_day,
                a13.[CATEGORY]  NAME0,
             a11.[DIA]  DIA,
             max(a14.[CAL_DATE])  CAL_DATE,
                a12.[Concepto]  Concepto,
                a13.[BRANDFAMILY_ID]  BRANDFAMILY_ID,
                max(a17.[NAME])  BRANDFAMILY_NAME,
                a12.[Item_type]  Item_type,
                sum(a11.[n_Item])  Item_num,
                sum(a11.[coste])  cost
from      Fact_Invest_Actuals_Daily           a11
                join        ITE.LU_Invest_Items      a12
                  on         (a11.[Investment_type] = a12.[Investment_type] and 
                a11.[Item_id] = a12.[Item_id])
                join        ITE.T_BRANDPACKS        a13
                  on         (a11.[BRANDPACK_ID] = a13.[BRANDPACK_ID])
                --join     ITE.T_DAY           a14
                --  on     (a11.[DIA] = a14.[DIA])
                join        ITE.LU_MONTH a15
                  on         (a14.[CAL_MONTH] = a15.[CAL_MONTH])
                join        ITE.LU_CLTE_1CANAL    a16
                  on         (a11.[Customer_ID] = a16.[Customer_ID])
                join        ITE.V_LU_BRANDFAMILIES         a17
                  on         (a13.[BRANDFAMILY_ID] = a17.[BRANDFAMILY_ID])
where   a15.[MES_ID] = convert(int,convert(varchar,getdate()-1,112))/100   -- CURRENT MONT – Delete foral DB
group by              a11.[Customer_ID],
                a11.[Investment_type],
                a11.[dia_inicio],
                a11.[dia_fin],
                a13.[CATEGORY],
                a11.[DIA],
                a12.[Concepto],
                a13.[BRANDFAMILY_ID],
                a12.[Item_type]
