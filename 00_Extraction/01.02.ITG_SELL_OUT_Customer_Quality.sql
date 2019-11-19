select	a11.[Customer_ID], isnull(a12.[Muestra_so_ok],0) OK_13M, isnull(a13.[Muestra_so_ok],0) OK_15M
from	ITE.LU_CLTE_1CANAL	a11
	left join	ITE.V_Lu_Muestra_SO_1Canal_12M	a12
	  on 	(a11.[Customer_ID] = a12.[Customer_ID])
	left join	ITE.V_Lu_Muestra_SO_1Canal_15M	a13
	  on 	(a11.[Customer_ID] = a13.[Customer_ID])
