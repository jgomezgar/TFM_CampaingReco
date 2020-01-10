# TFM_CampaingReco:

## Instrucciones

1.- Es necesario conservar toda la estructura de carpetas, ya que los resultados de calculos intermedios se vuelcan a fichero.

2.- **IMPORTANTE**:  Debido a que no es posible dar acceso directo a la BBDD, es necesario descargar los ficheros de datos desde el repositorio OneDrive y almacenarlos en la carpeta **"00_Extraction/Data"**: 

https://imptob-my.sharepoint.com/:f:/r/personal/esmijlmog_intleurope_imptobnet_com/Documents/TFM_CampaingReco?csf=1
(se trata de un vinculo restringido a las personas que me lo habeis solicitado)

3.- En One Drive Podeis encontrar los siguientes Ficheros:

* **TT_Sales_and_Invest.csv**: (2,6 GB) Extracción de BBDD, Generado mediante *"00_Extraction/Scripts/00_TT_extraction.py"*, contiene los datos sobre los que se generará y testará el modelo (target). Ubicarlo en *"00_Extraction/Data"*.

* **TT_Sales_and_Invest_REDUCIDO.csv**: (0,25 GB) Version reducida de la extracción, con la finalidad de testar el código incluido en *01_Preparation*, *02_Training*, *03_Evaluation*. Es necesario renombrar el fichero a  *TT_Sales_and_Invest.csv* , o cambiar el codigo en "00_Extraction/Scripts/00_TT_extraction.py". Optativo. Ubicarlo en *"00_Extraction/Data"*.

* **P_Sales_and_Invest.csv**: (13,4 GB) Extracción de BBDD, Generado mediante *"00_Extraction/Scripts/00_P_extraction.py"*, contiene los datos sobre los que se aplicara el modelo generado, tienen la misma estructura que *TT_Sales_and_Invest* excepto que desconocemos las ventas a consumidor (target). Ubicarlo en *"00_Extraction/Data"*.

* **P_Sales_and_Invest.7zip**: fichero comprimido en formarto 7zip (13,4GB-->1,2GB), si se opta por esta opción (recomendada), es necesario descomnprimir el fichero manualmente. Optativo. Ubicarlo en *"00_Extraction/Data"*.

* **customers_ok15.csv**: contiene el listado de los cliente con ventas a consumidor y calidad de datos suficiente, utilizado para seleccionar las muestras de train y test. Generado desde *"00_Extraction/Scripts/02_extraction_sellout_ok15.py"*. Ubicarlo en *"00_Extraction/Data"*.

* **Seasonality_Base.csv**: (1,8 GB) contiene la extracción de la estacionalidad de cada tupla cliente-Marca, utilizado para obtener las ventas limpias de estacionalidad y poder comparar periodos. Generado desde *"00_Extraction/Scripts/03_seasonality.py"*. Ubicarlo en *"00_Extraction/Data"*

4.- Ejecución: **executions_workflow.py** Lanza todos los scripts y procesos necesarios, permite especificar la parte del codigo a ejecutar:

* *00_Extraction*. Por defecto deshabilitada, ya que no existe acceso a la BBDD, para generar los CSVs.
* *01_Preparation*.
* *02_Training*. 
* *03_Evaluation*.

5.- Limpieza de ficheros: **remove_data.py**, una vez ejecutado todo el código, borrara todos los ficheros de datos (o los indicados si se especifica). El motivo de este escripts es que si se lanza una segunda vez la ejecucion, los ficheros de la anterior ejecucion pueden ocupara mas de 30 GB y limitar el espacio en disco, para la nueva ejecución.
