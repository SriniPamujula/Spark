// Databricks notebook source
import org.apache.spark.sql.functions._
import org.postgresql.Driver
import java.util.Properties

val sparkDF = spark.read.format("csv")
.option("header", "true")
.option("inferSchema", "true")
.option("delimiter",",") 
.load("dbfs:/FileStore/shared_uploads/pamujula@betterai.guru/US_COVID_SHORT_SAMPLE___Data_Challenge.csv")
 

// COMMAND ----------

 display(sparkDF)

// COMMAND ----------

val newDF = sparkDF.withColumn(sparkDF.columns(2), regexp_replace(col(sparkDF.columns(2)), ",", "").cast("int"))
.withColumn(sparkDF.columns(3), regexp_replace(col(sparkDF.columns(3)), ",", "").cast("int"))
.withColumn(sparkDF.columns(4), regexp_replace(col(sparkDF.columns(4)), ",", "").cast("int"))
.withColumn(sparkDF.columns(5), regexp_replace(col(sparkDF.columns(5)), ",", "").cast("int"))
.withColumn("covid_cases_rate", when(col("new_case") <= 20 ,"LOW")
      .when(col("new_case") > 50,"HIGH")
      .otherwise("MEDIUM"))
.withColumn("covid_deaths_rate", when(col("new_death") <= 5 ,"LOW")
      .when(col("new_death") > 10,"HIGH")
      .otherwise("MEDIUM"))


// COMMAND ----------

display(newDF)
newDF.printSchema

// COMMAND ----------

val database = "azcasestudy"
//val url: String = s"jdbc:postgresql://azcasestudy.postgres.database.azure.com/$database"
val url: String = s"jdbc:postgresql://aicasestudy.postgres.database.azure.com:5432/postgres?user=aicasestudy@aicasestudy&password=Azurepw1!&sslmode=require"
val tableName: String = "casestudy_spark"
//val user: String = "casestudy@azcasestudy"
//val password: String = "Azurepw1!"
val properties = new Properties()
//properties.setProperty("user", user)
//properties.setProperty("password", password)
properties.put("driver", "org.postgresql.Driver")
newDF.write.mode(SaveMode.Overwrite).jdbc(url, tableName, properties)



// COMMAND ----------

val pgConnectionProperties = new Properties()
pgConnectionProperties.put("user","casestudy")
pgConnectionProperties.put("password","casestudy")
val pgTable = "public.casestudy_table"
val pgDF = spark.read.jdbc("jdbc:postgresql://casestudy.cpklq9vljodx.us-east-1.rds.amazonaws.com/casestudy",pgTable,pgConnectionProperties)

