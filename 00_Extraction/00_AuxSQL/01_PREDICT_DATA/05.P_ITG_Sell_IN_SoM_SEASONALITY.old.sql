USE [STAGING_2]
GO

/****** Object:  Table [dbo].[XXX_P_Sell_Periods_10d]    Script Date: 01/08/2020 18:19:49 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO



-- Create Table Variable to hold results
CREATE TABLE [dbo].XXX_ITG_Sell_IN_SoM_SEASONALITY (
				ForecastKey int, 
				YEAR_ID int, 
				Saca_ID int, --  1-36
				[CAL_DATE] [date],
				[tercio] int,
				CUSTOMER_ID nvarchar(15),
				BrandFamily_ID nvarchar(15),
				Baseline_SOM numeric(38,17), 
				Smoothed_SOM numeric(38,17), 
				Trend numeric(38,17), 
				Seasonality numeric(38,17),
				Forward_Trend Numeric(38,17),
				Forecast Numeric(38,17))

Insert into [dbo].XXX_ITG_Sell_IN_SoM_SEASONALITY (Forecastkey, YEAR_ID, Saca_ID, CAL_DATE, tercio,CUSTOMER_ID, BrandFamily_ID, Baseline_SOM)
(
select 
	row_number() over (partition by t.[CUSTOMER_ID], t.[BRANDFAMILY_ID]order by t.CAL_DATE) ForecastKey,
	YEAR(t.CAL_DATE) YEAR_ID,
--	MONTH(t.CAL_DATE) MES_ID,
	MONTH(t.CAL_DATE)*3+tercio-3 Saca_ID,
	t.CAL_DATE, tercio,
	CUSTOMER_ID,
	BrandFamily_ID,
	isnull(SI_ITG_WSE/nullif(SI_MRKT_WSE,0),0) Baseline_SOM
from [dbo].XXX_P_Sell_Periods_10d t

where YEAR(t.CAL_DATE) > 2016
--order by  CUSTOMER_ID ,BrandFamily_ID, CAL_DATE
)





-- Update smoothed_SOM with Central Moving Average

	Update 
		[dbo].XXX_ITG_Sell_IN_SoM_SEASONALITY 
	SET 
		Smoothed_SOM = MovAvg.Smoothed_SOM
	FROM(
		SELECT 
			a.ForecastKey as FKey,
			a.CUSTOMER_ID as Cust,
			a.BrandFamily_ID as BF, 
			AVG(b.baseline_SOM) Smoothed_SOM
		FROM 
			[dbo].XXX_ITG_Sell_IN_SoM_SEASONALITY a
		INNER JOIN 
			[dbo].XXX_ITG_Sell_IN_SoM_SEASONALITY b 
		ON 
			a.CUSTOMER_ID = b.CUSTOMER_ID
			AND a.BrandFamily_ID = b.BrandFamily_ID 
			AND	(a.ForecastKey - b.ForecastKey) BETWEEN -6 AND 6
		GROUP BY
			a.ForecastKey,
			a.CUSTOMER_ID,
			a.BrandFamily_ID) MovAvg
	WHERE 
		CUSTOMER_ID =MovAvg.Cust
		AND BrandFamily_ID = MovAvg.BF
		AND ForecastKey = MovAvg.FKey
	


	-- Create table to store calculations by Item
	DECLARE @Formula as Table(
					CUSTOMER_ID nvarchar(15),
					BrandFamily_ID nvarchar(15),
					Counts int,  
					SumX Numeric(14,4), 
					SumY Numeric(14,4),
					SumXY Numeric(14,4), 
					SumXsqrd Numeric(14,4), 
					b Numeric(38,17), 
					a Numeric(38,17))
	
	INSERT INTO @Formula (CUSTOMER_ID, BrandFamily_ID, Counts, SumX, SumY, SumXY, SumXsqrd)	
		(SELECT 
			CUSTOMER_ID,
			BrandFamily_ID,
			COUNT(*),
			sum(ForecastKey),
			sum(Smoothed_SOM),
			sum(Smoothed_SOM * ForecastKey),
			sum(power(ForecastKey,2)) 
		FROM 
			[dbo].XXX_ITG_Sell_IN_SoM_SEASONALITY
		WHERE 
			Smoothed_SOM IS NOT NULL
		GROUP BY 
			CUSTOMER_ID, BrandFamily_ID)
		
		-- Calculate B (Slope)
		UPDATE @Formula 
		SET
			b = ((tb.counts * tb.sumXY)-(tb.sumX * tb.sumY))/ (tb.Counts * tb.sumXsqrd - power(tb.sumX,2))
		FROM
			(SELECT CUSTOMER_ID as XCUSTOMER_ID, BrandFamily_ID as XBrandFamily_ID, Counts, SumX, SumY, SumXY, SumXsqrd FROM @Formula) tb
		WHERE CUSTOMER_ID = tb.XCUSTOMER_ID and 
					BrandFamily_ID = tb.XBrandFamily_ID
		
		--Calculate A (Y Intercept)
		UPDATE 
			@Formula 
		SET
			a = ((tb2.sumY - tb2.b * tb2.sumX) / tb2.Counts)
		FROM
			(SELECT CUSTOMER_ID as XCUSTOMER_ID, BrandFamily_ID as XBrandFamily_ID, Counts, SumX, SumY, SumXY, SumXsqrd, b FROM @Formula) tb2
		WHERE CUSTOMER_ID = tb2.XCUSTOMER_ID and 
					BrandFamily_ID = tb2.XBrandFamily_ID
		
		-- Update Historical Trend and Seasonality
		--y = a + bx
		--Forecast = Y Intercept + (Slope * ForecastKey)
		UPDATE 
			[dbo].XXX_ITG_Sell_IN_SoM_SEASONALITY 
		SET 
			Trend = A + (B * ForecastKey),
			Seasonality = CASE WHEN Baseline_SOM = 0 
				THEN 1 ELSE Baseline_SOM /(A + (B * ForecastKey)) END
		FROM
		 (SELECT CUSTOMER_ID as XCUSTOMER_ID, BrandFamily_ID as XBrandFamily_ID, Counts, SumX, SumY, SumXY, SumXsqrd, b, a FROM @Formula) TrendUpdate
		WHERE CUSTOMER_ID = TrendUpdate.XCUSTOMER_ID and 
					BrandFamily_ID = TrendUpdate.XBrandFamily_ID

