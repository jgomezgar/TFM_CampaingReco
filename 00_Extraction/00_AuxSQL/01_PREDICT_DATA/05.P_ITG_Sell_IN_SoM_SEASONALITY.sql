/*################### 7 min #################################################*/
with Sell_IN_SoM as (
select 
	row_number() over (partition by t.[CUSTOMER_ID], t.[BRANDFAMILY_ID]order by t.CAL_DATE) ForecastKey,
	YEAR(t.CAL_DATE) YEAR_ID,
--	MONTH(t.CAL_DATE) MES_ID,
	MONTH(t.CAL_DATE)*3+tercio-3 Saca_ID,
	t.CAL_DATE, tercio,
	CUSTOMER_ID,
	BrandFamily_ID,
	isnull(SI_ITG_WSE/nullif(SI_MRKT_WSE,0),0) Baseline_SOM
from [STAGING_2].[dbo].XXX_P_Sell_Periods_10d t

where t.CAL_DATE >= '2016-12-01'
--order by  CUSTOMER_ID ,BrandFamily_ID, CAL_DATE
)




, Sell_IN_SoM_MovAvg as (
		SELECT 
			a.ForecastKey ,
			a.YEAR_ID,
			a.SACA_ID,
			a.CAL_DATE, 
			a.tercio,
			a.CUSTOMER_ID ,
			a.BrandFamily_ID, 
			a.Baseline_SOM,
			AVG(b.baseline_SOM) Smoothed_SOM
		FROM 
			Sell_IN_SoM a
		INNER JOIN 
			Sell_IN_SoM b 
		ON 
			a.CUSTOMER_ID = b.CUSTOMER_ID
			AND a.BrandFamily_ID = b.BrandFamily_ID 
			AND	(a.ForecastKey - b.ForecastKey) BETWEEN -6 AND 6
		GROUP BY
			a.ForecastKey ,
			a.YEAR_ID,
			a.SACA_ID,
			a.CAL_DATE, 
			a.tercio,
			a.CUSTOMER_ID ,
			a.BrandFamily_ID, 
			a.Baseline_SOM
)


	
, Sell_IN_SoM_MovAvgXY as (

		SELECT 
			CUSTOMER_ID,
			BrandFamily_ID,
			COUNT(*) Counts,
			sum(ForecastKey) SumX,
			sum(Smoothed_SOM) SumY,
			sum(Smoothed_SOM * ForecastKey) SumXY,
			sum(power(ForecastKey,2)) SumXsqrd,
			b = ((COUNT(*) * sum(Smoothed_SOM * ForecastKey))-(sum(ForecastKey) * sum(Smoothed_SOM)))/ nullif(COUNT(*)  * sum(power(ForecastKey,2)) - power(sum(ForecastKey),2),0),
			a = ((sum(Smoothed_SOM) - 
			--b 
			((COUNT(*) * sum(Smoothed_SOM * ForecastKey))-(sum(ForecastKey) * sum(Smoothed_SOM)))/ nullif(COUNT(*)  * sum(power(ForecastKey,2)) - power(sum(ForecastKey),2),0)
			* sum(ForecastKey)) / nullif(COUNT(*),0))
			
		FROM 
			Sell_IN_SoM_MovAvg
		WHERE 
			Smoothed_SOM IS NOT NULL
		GROUP BY 
			CUSTOMER_ID, BrandFamily_ID
)
		
		
		-- Update Historical Trend and Seasonality
		--y = a + bx
		--Forecast = Y Intercept + (Slope * ForecastKey)
select MA.*,
	Trend = A + (B * ForecastKey),
	Seasonality = CASE WHEN Baseline_SOM = 0 THEN 1 
				  ELSE Baseline_SOM /nullif(A + (B * ForecastKey),0) END
into [dbo].XXX_ITG_Sell_IN_SoM_SEASONALITY 
FROM Sell_IN_SoM_MovAvg MA join Sell_IN_SoM_MovAvgXY XY
on  MA.CUSTOMER_ID = XY.CUSTOMER_ID and 
	MA.BrandFamily_ID = XY.BrandFamily_ID
	
where YEAR(CAL_DATE) > 2016
	

