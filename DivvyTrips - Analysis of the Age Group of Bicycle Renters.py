# Databricks notebook source
# MAGIC %sql -- criar banco de dados se não existir
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS divvytrips_db;

# COMMAND ----------

import pandas

# importando os datasets do bucket S3
dataframe_1 = pandas.read_csv('https://awsdatasets.s3-ap-southeast-1.amazonaws.com/divvy/Divvy_Trips_2019_Q1.csv');
dataframe_2 = pandas.read_csv('https://awsdatasets.s3-ap-southeast-1.amazonaws.com/divvy/Divvy_Trips_2019_Q2.csv');
dataframe_3 = pandas.read_csv('https://awsdatasets.s3-ap-southeast-1.amazonaws.com/divvy/Divvy_Trips_2019_Q3.csv');
dataframe_4 = pandas.read_csv('https://awsdatasets.s3-ap-southeast-1.amazonaws.com/divvy/Divvy_Trips_2019_Q4.csv');

# renomeando colunas com o nome difente dos outros datasets
dataframe_2.columns = ['trip_id','start_time','end_time','bikeid','tripduration','from_station_id',
                       'from_station_name','to_station_id','to_station_name','usertype','gender','birthyear'];

# unificando os dataframes
df_full = pandas.concat([dataframe_1, dataframe_2, dataframe_3, dataframe_4]);

df_full.columns = ['id','tempo_inicial','tempo_final','id_bicicleta','duracao_viagem','id_estacao_inicial',
              'estacao_inicial','id_estacao_final','estacao_final','tipo_usuario','genero','aniversario'];

# convertendo dataframe pandas para spark
dfs = spark.createDataFrame(df_full);

#escrevendo o dataframe em delta
dfs.write.format("delta").mode("overwrite").partitionBy("genero").save("/delta/divvytrips")
dbs_delta = spark.read.format("delta").load("/delta/divvytrips");

# COMMAND ----------

# total de linhas
print(dbs_delta.count())

# COMMAND ----------

#visualizando os dados
display(dbs_delta)

# COMMAND ----------

# removendo colunas
dbs_delta = dbs_delta.drop("id_bicicleta", "id_estacao_inicial", "id_estacao_final")

# COMMAND ----------

# a coluna "duração_viagem" exibe o tempo em segundos

display(dbs_delta)

# COMMAND ----------

# MAGIC %sql --criando uma tabela para a manipulação de dados com SQL
# MAGIC 
# MAGIC USE divvytrips_db;
# MAGIC 
# MAGIC DROP TABLE IF EXISTS tb_divvytrips;
# MAGIC CREATE TABLE tb_divvytrips
# MAGIC USING delta
# MAGIC LOCATION '/delta/divvytrips';

# COMMAND ----------

# DBTITLE 0,Bike Rentals by Age: Men vs. Women
# MAGIC %sql -- média de aluguel de bicicletas por idade durante o ano
# MAGIC 
# MAGIC SELECT 
# MAGIC (YEAR(CURRENT_TIMESTAMP) - CAST(aniversario AS INT)) AS idade,
# MAGIC CASE WHEN genero = 'Male' THEN 'masculino' ELSE 'feminino' END AS generos,
# MAGIC COUNT(*) AS `números de alugueis`
# MAGIC FROM tb_divvytrips
# MAGIC WHERE (aniversario IS NOT NULL) AND (genero IS NOT NULL)
# MAGIC GROUP BY idade,generos,(YEAR(CURRENT_TIMESTAMP) - CAST(aniversario AS INT))
# MAGIC HAVING COUNT(*) > 0
# MAGIC ORDER BY idade
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql -- média de aluguéis por trimestre
# MAGIC 
# MAGIC SELECT 
# MAGIC QUARTER(CAST(tempo_inicial AS DATE)) AS trimestres,
# MAGIC CASE 
# MAGIC     WHEN MONTH(CAST(tempo_inicial AS DATE))='01' THEN 'janeiro'
# MAGIC     WHEN MONTH(CAST(tempo_inicial AS DATE))='02' THEN 'fevereiro'
# MAGIC     WHEN MONTH(CAST(tempo_inicial AS DATE))='03' THEN 'marco'
# MAGIC     WHEN MONTH(CAST(tempo_inicial AS DATE))='04' THEN 'abril'
# MAGIC     WHEN MONTH(CAST(tempo_inicial AS DATE))='05' THEN 'maio'
# MAGIC     WHEN MONTH(CAST(tempo_inicial AS DATE))='06' THEN 'junho'
# MAGIC     WHEN MONTH(CAST(tempo_inicial AS DATE))='07' THEN 'julho'
# MAGIC     WHEN MONTH(CAST(tempo_inicial AS DATE))='08' THEN 'agosto'
# MAGIC     WHEN MONTH(CAST(tempo_inicial AS DATE))='09' THEN 'setembro'
# MAGIC     WHEN MONTH(CAST(tempo_inicial AS DATE))='10' THEN 'outubro'
# MAGIC     WHEN MONTH(CAST(tempo_inicial AS DATE))='11' THEN 'novembro'
# MAGIC     WHEN MONTH(CAST(tempo_inicial AS DATE))='12' THEN 'dezembro'
# MAGIC END meses,
# MAGIC CASE WHEN genero = 'Male' THEN 'masculino' ELSE 'feminino' END AS generos,
# MAGIC COUNT(*) AS `números de alugueis`
# MAGIC FROM tb_divvytrips
# MAGIC WHERE (aniversario IS NOT NULL) 
# MAGIC AND (genero IS NOT NULL) 
# MAGIC AND CAST(aniversario AS INT) >= 1900
# MAGIC GROUP BY meses,trimestres,generos
# MAGIC ORDER BY trimestres

# COMMAND ----------

# MAGIC %sql -- % de aluguéis por genero
# MAGIC 
# MAGIC SELECT
# MAGIC CASE WHEN tipo_usuario = 'Subscriber' THEN 'assinante' ELSE 'não-assinante' END AS `tipo de vinculo`,
# MAGIC CASE WHEN genero = 'Male' THEN 'masculino' ELSE 'feminino' END generos,
# MAGIC COUNT(*) AS  `números de alugueis`
# MAGIC FROM tb_divvytrips
# MAGIC WHERE (aniversario IS NOT NULL) 
# MAGIC AND (genero IS NOT NULL) 
# MAGIC AND CAST(aniversario AS INT) >= 1900
# MAGIC GROUP BY `tipo de vinculo`,generos
# MAGIC ORDER BY `tipo de vinculo`

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC criado por [Romerito morais](https://www.linkedin.com/in/romeritomorais/)
