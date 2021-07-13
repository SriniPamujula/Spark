// Databricks notebook source
// MAGIC %run "/Users/srinivas.pamujula@alteonhealth.com/utilities/SQLUtilities_14"

// COMMAND ----------

display(df)

// COMMAND ----------

import SqlWriter._
def processFilesBON(path: String): DataFrame = {
//Process Bon Secours excel files  
  val file =  path.replace("dbfs:", "") 
  println(file)
  
  //read the file and create the temp view
 spark.read.format("com.crealytics.spark.excel")
  .option("header", "true")
  .option("treatEmptyValuesAsNulls", "false")
  .option("inferSchema", "false")
  .option("addColorColumns", "false")
  .option("dataAddress", "A4") 
  .load(file)
  .createOrReplaceTempView("facility_xls")
  

 // return df_excel
 //incoming folder
 //val loc = file.substring(15,18)
//archive folder
  val loc = file.substring(23,26)
//  val loc = file.substring(24,27)

//
  val qry1 = s"""select 
                  |     '${loc}' as  Location
                  |     ,"ED" PlaceOfService
                  |     ,`E-MRN`  MRN
                  |     ,`HAR #`  VisitID
                  |     ,substring(`ED PROVIDER`,instr(`ED PROVIDER`,'[')+1,(instr(`ED PROVIDER`,']')-1-instr(`ED PROVIDER`,'[')) ) ProviderID
                  |     , `ED PROVIDER` ProviderName
                  |     , initcap(lower(`PATIENT LAST NAME`)) PatientLastName
                  |     , initcap(lower(`PATIENT FIRST NAME`)) PatientFirstName
                  |     , ifnull(`BIRTH DATE`,NULL) PatientDOB
                  |     ,GENDER PatientGender
                  |     ,ifnull(`ED DISPOSITION`,NULL) Disposition
                  |     ,ifnull(left(`ED ARRIVAL TIME`,8),NULL) ArrivalDate  
                  |     ,ifnull(`ED ARRIVAL TIME`,NULL) ArrivalDateTime
                  |     ,case when `TRIAGE START TIME` = "N/A" then NULL else  `TRIAGE START TIME` end TriageDateTime
                  |     ,case when `ED ROOMED TIME` = "N/A" then NULL else `ED ROOMED TIME` end  BedDateTime
                  |     ,case when `MD FIRST ASG/CARE` = "N/A" then NULL else `MD FIRST ASG/CARE` end  ProviderDateTime
                  |     ,case when `DISPO/DECISION TIME` = "N/A" then NULL else `DISPO/DECISION TIME`  end  DispositionDateTime
                  |     ,case when `ED DEPARTURE TIME` = "N/A" then NULL else `ED DEPARTURE TIME` end DepartureDateTime
                  |     ,`CHIEF COMPLAINT` ChiefComplaint 
                  |     ,`ADT DESTINATION (POST-ED)` AdmitFloor
                  |     ,`ACUITY LEVEL` TriageLevel
                  |     ,'' Nurse
                  |     ,'' CustCol01
                  |     ,'' CustCol02
                  |     ,'' CustCol03
                  |     ,'' CustCol04
                  |     ,'' CustCol05
                  |     ,'' CustCol06
                  |     ,current_timestamp() InsertedDate
                  |     , '${file}' FileName
                  | from facility_xls
                  | where`E-MRN` is not null and `HAR #` is not null""".stripMargin

//return the transformed dataframe output
spark.sql(qry1)
}


// COMMAND ----------


//val BON_files = df.filter($"path".like("%/mnt/troughput/archive/DEP%") and $"path".like("%DAILY%") )

val BON_files = df.filter($"path".like("%/mnt/troughput/archive/DEP%") and $"path".like("%DAILY%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("DEP daily File not available " )
}

val BON_files7 = df.filter($"path".like("%/mnt/troughput/archive/DEP%") and $"path".like("%7 DAYS%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files7.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files7) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
    }
    catch{
      case _: Throwable => println("Exception: " +  fileArray.toString)
    }
  }
}else{
  println("DEP 7 Days File not available: " )
}


// COMMAND ----------


val BON_files = df.filter($"path".like("%/mnt/troughput/archive/HAR%") and $"path".like("%DAILY%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("HAR daily File not available " )
}

val BON_files7 = df.filter($"path".like("%/mnt/troughput/archive/HAR%") and $"path".like("%7 DAYS%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files7.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files7) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
    }
    catch{
      case _: Throwable => println("Exception: " +  fileArray.toString)
    }
  }
}else{
  println("HAR 7 Days File not available: " )
}



// COMMAND ----------


val BON_files = df.filter($"path".like("%/mnt/troughput/archive/MAR%") and $"path".like("%DAILY%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("MAR daily File not available " )
}

val BON_files7 = df.filter($"path".like("%/mnt/troughput/archive/MAR%") and $"path".like("%7 DAYS%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files7.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files7) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
    }
    catch{
      case _: Throwable => println("Exception: " +  fileArray.toString)
    }
  }
}else{
  println("MAR 7 Days File not available: " )
}



// COMMAND ----------


val BON_files = df.filter($"path".like("%/mnt/troughput/archive/MVW%") and $"path".like("%DAILY%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("MVW daily File not available " )
}

val BON_files7 = df.filter($"path".like("%/mnt/troughput/archive/MVW%") and $"path".like("%7 DAYS%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files7.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files7) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
    }
    catch{
      case _: Throwable => println("Exception: " +  fileArray.toString)
    }
  }
}else{
  println("MVW 7 Days File not available: " )
}


// COMMAND ----------


//val BON_files = df.filter($"path".like("%/mnt/troughput/archive/MEM%") and $"path".like("%DAILY%") )
val BON_files = df.filter($"path".like("%RPT5363_EMA PROCESS TIMES DATA EXTRACT (MRM) - DAILY_2021-03-30-06-20-21.xls%") and $"path".like("%DAILY%") ) 
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("MEM daily File not available " )
}
/*
val BON_files7 = df.filter($"path".like("%/mnt/troughput/archive/MEM%") and $"path".like("%7 DAYS%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files7.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files7) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
    }
    catch{
      case _: Throwable => println("Exception: " +  fileArray.toString)
    }
  }
}else{
  println("MEM 7 Days File not available: " )
}

*/

// COMMAND ----------


val BON_files = df.filter($"path".like("%/mnt/troughput/archive/RAP%") and $"path".like("%DAILY%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("RAP daily File not available " )
}

val BON_files7 = df.filter($"path".like("%/mnt/troughput/archive/RAP%") and $"path".like("%7 DAYS%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files7.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files7) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
    }
    catch{
      case _: Throwable => println("Exception: " +  fileArray.toString)
    }
  }
}else{
  println("RAP 7 Days File not available: " )
}



// COMMAND ----------


val BON_files = df.filter($"path".like("%/mnt/troughput/archive/RIC%") and $"path".like("%DAILY%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("RIC daily File not available " )
}

val BON_files7 = df.filter($"path".like("%/mnt/troughput/archive/RIC%") and $"path".like("%7 DAYS%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files7.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files7) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
    }
    catch{
      case _: Throwable => println("Exception: " +  fileArray.toString)
    }
  }
}else{
  println("RIC 7 Days File not available: " )
}



// COMMAND ----------


val BON_files = df.filter($"path".like("%/mnt/troughput/archive/SHF%") and $"path".like("%DAILY%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("SHF daily File not available " )
}

val BON_files7 = df.filter($"path".like("%/mnt/troughput/archive/SHF%") and $"path".like("%7 DAYS%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files7.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files7) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
    }
    catch{
      case _: Throwable => println("Exception: " +  fileArray.toString)
    }
  }
}else{
  println("SHF 7 Days File not available: " )
}



// COMMAND ----------


val BON_files = df.filter($"path".like("%/mnt/troughput/archive/SVR%") and $"path".like("%DAILY%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("SVR daily File not available " )
}

val BON_files7 = df.filter($"path".like("%/mnt/troughput/archive/SVR%") and $"path".like("%7 DAYS%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files7.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files7) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
    }
    catch{
      case _: Throwable => println("Exception: " +  fileArray.toString)
    }
  }
}else{
  println("SVR 7 Days File not available: " )
}



// COMMAND ----------


val BON_files = df.filter($"path".like("%/mnt/troughput/archive/SRM%") and $"path".like("%DAILY%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("SRM daily File not available " )
}

val BON_files7 = df.filter($"path".like("%/mnt/troughput/archive/SRM%") and $"path".like("%7 DAYS%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files7.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files7) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
    }
    catch{
      case _: Throwable => println("Exception: " +  fileArray.toString)
    }
  }
}else{
  println("SRM 7 Days File not available: " )
}



// COMMAND ----------


val BON_files = df.filter($"path".like("%/mnt/troughput/archive/SEC%") and $"path".like("%DAILY%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("SEC daily File not available " )
}

val BON_files7 = df.filter($"path".like("%/mnt/troughput/archive/SEC%") and $"path".like("%7 DAYS%") )
            .select("path").rdd.map(r => r(0)).collect()
if (BON_files7.size > 0){
//loop through each bon file -> read, process, write
  for(fileArray <- BON_files7) {
    try{
    writeToSQL(processFilesBON(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
    }
    catch{
      case _: Throwable => println("Exception: " +  fileArray.toString)
    }
  }
}else{
  println("SEC 7 Days File not available: " )
}



// COMMAND ----------

import SqlWriter._

def processFilesALX(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 

  sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter","|")    
     .load(file)
    .createOrReplaceTempView("facility_xls")

 // val loc = file.substring(15,18)
 // println(loc)

  val qry1 = """select 
                    'ALX' as  Location
                    ,"ED" PlaceOfService
                    ,String(`MRN`) MRN
                    ,String(`HAR`) VisitID
                    , '' ProviderID
                    , `ED TT - PA&NP` ProviderName
                    , initcap(lower(`Last Name`)) PatientLastName
                    , initcap(lower(`First Name`)) PatientFirstName
                    , ifnull(`DOB`,NULL) PatientDOB
                    , `Sex` PatientGender
                    , ifnull(`ED Disposition`,'' ) Disposition
                    , ifnull(SubString(`Arrv Date/Time`,2,10),NULL) ArrivalDate  
                    , ifnull(to_Date(substring(`Arrv Date/Time`,2,16),'MM/dd/yyyy HHmm'),NULL) ArrivalDateTime
                    , '' TriageDateTime
                --  , ifnull(to(substring(`Arrv Date/Time`,2,16),'MM/dd/yyyy HHmm'),NULL)  BedDateTime
                    
                    , to_timestamp(substring(`Arrv Date/Time`,2,16),'MM/dd/yyyy HHmm')  BedDateTime

                    
                    ,ifnull(to_Date(substring(`Arrv Date/Time`,2,16),'MM/dd/yyyy HHmm'),NULL)  ProviderDateTime
                    ,ifnull(to_Date(substring(`Arrv Date/Time`,2,16),'MM/dd/yyyy HHmm'),NULL)  DispositionDateTime
                    ,ifnull(to_Date(substring(`Arrv Date/Time`,2,16),'MM/dd/yyyy HHmm'),NULL) DepartureDateTime
                    , `Chief Complaint` ChiefComplaint 
                    ,case when `unit` = 'AX EMERGENCY DEPT [102014050]' then '' else `Unit` end AdmitFloor
                    ,`Acuity Abbr` TriageLevel
                    ,'' Nurse
                    ,'' CustCol01
                    ,'' CustCol02
                    ,'' CustCol03
                    ,'' CustCol04
                    ,'' CustCol05
                    ,'' CustCol06
                    ,current_timestamp() InsertedDate
                    , '${file}' FileName
                    from `facility_xls`
                    where`MRN` IS NOT NULL AND `HAR` IS NOT NULL  ;""".stripMargin
//  println(qry1)
spark.sql(qry1)
}


// COMMAND ----------

// MAGIC %sql
// MAGIC select 
// MAGIC                     'ALX' as  Location
// MAGIC                     ,"ED" PlaceOfService
// MAGIC                     ,String(`MRN`) MRN
// MAGIC                     ,String(`HAR`) VisitID
// MAGIC                     , '' ProviderID
// MAGIC                     , `ED TT - PA&NP` ProviderName
// MAGIC                     , initcap(lower(`Last Name`)) PatientLastName
// MAGIC                     , initcap(lower(`First Name`)) PatientFirstName
// MAGIC                     , ifnull(`DOB`,NULL) PatientDOB
// MAGIC                     , `Sex` PatientGender
// MAGIC                     , ifnull(`ED Disposition`,'' ) Disposition
// MAGIC                     , ifnull(SubString(`Arrv Date/Time`,2,10),NULL) ArrivalDate  
// MAGIC                     , ifnull(to_Date(substring(`Arrv Date/Time`,2,16),'MM/dd/yyyy HHmm'),NULL) ArrivalDateTime
// MAGIC                     , '' TriageDateTime
// MAGIC                 --  , ifnull(to(substring(`Arrv Date/Time`,2,16),'MM/dd/yyyy HHmm'),NULL)  BedDateTime
// MAGIC                     
// MAGIC                     , to_timestamp(substring(`Arrv Date/Time`,2,16),'MM/dd/yyyy HHmm')  BedDateTime
// MAGIC                     ,to_timestamp(substring(`Arrv Date/Time`,2,16),'MM/dd/yyyy HHmm') + interval 10 minutes as ArrivalPlusMinutes
// MAGIC                     , cast(`Arrival to Room Hosp Rpt` as INT) test
// MAGIC                      ,to_timestamp(substring(`Arrv Date/Time`,2,16),'MM/dd/yyyy HHmm') + interval 'cast(`Arrival to Room Hosp Rpt` as INT)' minutes as ArrivalPlusMinutes2
// MAGIC 
// MAGIC                     
// MAGIC                     ,ifnull(to_Date(substring(`Arrv Date/Time`,2,16),'MM/dd/yyyy HHmm'),NULL)  ProviderDateTime
// MAGIC                     ,ifnull(to_Date(substring(`Arrv Date/Time`,2,16),'MM/dd/yyyy HHmm'),NULL)  DispositionDateTime
// MAGIC                     ,ifnull(to_Date(substring(`Arrv Date/Time`,2,16),'MM/dd/yyyy HHmm'),NULL) DepartureDateTime
// MAGIC                     , `Chief Complaint` ChiefComplaint 
// MAGIC                     ,case when `unit` = 'AX EMERGENCY DEPT [102014050]' then '' else `Unit` end AdmitFloor
// MAGIC                     ,`Acuity Abbr` TriageLevel
// MAGIC                     ,'' Nurse
// MAGIC                     ,'' CustCol01
// MAGIC                     ,'' CustCol02
// MAGIC                     ,'' CustCol03
// MAGIC                     ,'' CustCol04
// MAGIC                     ,'' CustCol05
// MAGIC                     ,'' CustCol06
// MAGIC                     ,current_timestamp() InsertedDate
// MAGIC                     , '${file}' FileName
// MAGIC                     from `facility_xls`
// MAGIC                     where`MRN` IS NOT NULL AND `HAR` IS NOT NULL  ;

// COMMAND ----------


val ALX_files = df.filter($"path".like("%/mnt/troughput/archive/ALX/IMM_ALX_TP202101201115.txt%")  )
            .select("path").rdd.map(r => r(0)).collect()
if (ALX_files.size > 0){
//loop through each ALX file -> read, process, write
  for(fileArray <- ALX_files) {
    try{
    writeToSQL(processFilesALX(fileArray.toString), "pre_stg", "Throughput_test",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("ALX daily File not available " )
}


// COMMAND ----------

import SqlWriter._

def processFilesHPX(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 

  sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter","|")    
     .load(file)
    .createOrReplaceTempView("facility_xls")

  //val loc = file.substring(15,18)
  //println(loc)

  
 val qry1 = s"""select 
          'HPX' as  Location
          ,"ED" PlaceOfService
          ,String(`MRN`) MRN
          ,String(`HAR`)  VisitID
          ,'' ProviderID
          , ifnull(`ED TT - PA&NP`,`First ED Provider`) ProviderName
          , initcap(lower(`Last Name`)) PatientLastName
          , initcap(lower(`First Name`)) PatientFirstName
          , concat(split(replace(`DOB`,',',''), ' ')[2],'-',split(replace(`DOB`,',',''), ' ')[0] ,'-' ,split(replace(`DOB`,',',''), ' ')[1])  PatientDOB
          , SubString(`Sex`,1,1) PatientGender
          , `ED Disposition` Disposition
           , ifnull(SubString(`Arrv Date/Time`,2,10),NULL) ArrivalDate  
          , ifnull(to_Date(substring(`Arrv Date/Time`,2,16),'MM/dd/yyyy HHmm'),NULL) ArrivalDateTime
          , '' TriageDateTime
          , ifnull(to_Date(substring(`Arrv Date/Time`,2,16),'MM/dd/yyyy HHmm'),NULL)  BedDateTime
          ,ifnull(to_Date(substring(`Arrv Date/Time`,2,16),'MM/dd/yyyy HHmm'),NULL)  ProviderDateTime
          ,ifnull(to_Date(substring(`Arrv Date/Time`,2,16),'MM/dd/yyyy HHmm'),NULL)  DispositionDateTime
          ,ifnull(to_Date(substring(`Arrv Date/Time`,2,16),'MM/dd/yyyy HHmm'),NULL) DepartureDateTime
          , `Chief Complaint` ChiefComplaint
          ,case when `unit` = 'AX EMERGENCY DEPT [102014050]' then '' else `Unit` end AdmitFloor
          ,`Acuity Abbr` TriageLevel
          ,'' Nurse
          ,'' CustCol01
                    ,'' CustCol02
                    ,'' CustCol03
                    ,'' CustCol04
                    ,'' CustCol05
                    ,'' CustCol06
          ,current_timestamp() InsertedDate
          ,  '${file}' FileName
      from `facility_xls`
      where`MRN` IS NOT NULL AND `HAR` IS NOT NULL  ;""".stripMargin
  //println(qry1)
  spark.sql(qry1)
  //println(results)
  
}

// COMMAND ----------

val HPX_files = df.filter($"path".like("%/mnt/troughput/archive/HPX%")  )
            .select("path").rdd.map(r => r(0)).collect()
if (HPX_files.size > 0){
//loop through each HPX file -> read, process, write
  for(fileArray <- HPX_files) {
    try{
    writeToSQL(processFilesHPX(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("HPX daily File not available " )
}

// COMMAND ----------

import SqlWriter._

def processFilesCAL(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 

  sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter","|")    
     .load(file)
    .createOrReplaceTempView("facility_xls")

  //val loc = file.substring(15,18)
  //println(loc)
  val qry1 = s"""select 
          'CAL' as  Location
          ,"ED" PlaceOfService
          ,String(`MR#`) MRN
          ,String(`Visit#`)  VisitID
          , String(`Physician NPI/ID`) ProviderID
          , ifnull(replace(`Doctor Extender Name`,',',''),replace(`Physician Name`,',','')) ProviderName
          , initcap(lower(`Last Name`)) PatientLastName
          , initcap(lower(`First Name`)) PatientFirstName
          , `Birthdate` PatientDOB
          , `Sex` PatientGender
          , ifnull(`Dispostion Name`,'') Disposition
          , ifnull(`Date`,NULL) ArrivalDate   
          , ifnull(`Arrival`,NULL) ArrivalDateTime
          , ifnull(`Triage`,NULL) TriageDateTime
          , ifnull(`Bed`,NULL)  BedDateTime
          , ifnull(`Provider`,NULL)  ProviderDateTime
          , ifnull(`Disposition`,NULL)  DispositionDateTime
          , ifnull(`Exit`,NULL)  DepartureDateTime
          , `Chief Complaint` ChiefComplaint 
          , '' AdmitFloor
          ,`Triage Level` TriageLevel
          ,'' Nurse
          ,'' CustCol01
                    ,'' CustCol02
                    ,'' CustCol03
                    ,'' CustCol04
                    ,'' CustCol05
                    ,'' CustCol06
          ,current_timestamp() InsertedDate
          , '${file}' FileName
      from 
      `facility_xls`
      where `MR#` IS NOT NULL AND `Visit#` IS NOT NULL  ;""".stripMargin
spark.sql(qry1)
}

// COMMAND ----------


val CAL_files = df.filter($"path".like("%/mnt/troughput/archive/CAL%") )
            .select("path").rdd.map(r => r(0)).collect()
if (CAL_files.size > 0){
//loop through each CAL file -> read, process, write
  for(fileArray <- CAL_files) {
    try{
    writeToSQL(processFilesCAL(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("CAL daily File not available " )
}



// COMMAND ----------

import SqlWriter._
def processFilesCAR(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 
  println(file)
  sqlContext.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter","|")
  .option("ignoreTrailingWhiteSpace",true)    
  .load(file)
  .createOrReplaceTempView("facility_xls")

  //val loc = file.substring(15,18)
 // println(loc)
  val qry1 = s"""select 
          'CAR' as  Location
          ,'ED' PlaceOfService
          ,String(`MRN`) MRN
          ,String(`VISIT`) VisitID
          ,`PHYSICIAN_NPI` ProviderID
          , `PHYSICIAN` ProviderName 
          , `PATIENT_LAST_NAME` PatientLastName
          , `PATIENT_FIRST_NAME` PatientFirstName
          , `PATIENT_DATE_OF_BIRTH` PatientDOB
          , `GENDER` PatientGender
          , `DISPOSITION_DESCRIPTION` Disposition 
          , ifnull(`ARRIVAL`,NULL) ArrivalDate  
          , ifnull(`ARRIVAL`,NULL) ArrivalDateTime
          , ifnull(`TRIAGE`,NULL) TriageDateTime
          , ifnull(`BED`,NULL)  BedDateTime
          , ifnull(`PROVIDER`,NULL)  ProviderDateTime
          , ifnull(`DISPOSITION`,NULL)  DispositionDateTime
          , ifnull(`EXIT_DT_TM`,NULL)  DepartureDateTime
          , `CHIEF_COMPLAINT` ChiefComplaint 
          , `ADMIT_FLOOR` AdmitFloor
          ,String(`TRIAGE_LEVEL`) TriageLevel
          ,`DISCHARGE_NURSE_NAME` Nurse
          ,'' CustCol01
                    ,'' CustCol02
                    ,'' CustCol03
                    ,'' CustCol04
                    ,'' CustCol05
                    ,'' CustCol06
           ,current_timestamp() InsertedDate
          ,'${file}' FileName
      from 
      `facility_xls`
        ;""".stripMargin
spark.sql(qry1)

  
}

// COMMAND ----------


val CAR_files = df.filter($"path".like("%/mnt/troughput/archive/CAR/car_ed_alteon_extract_20210327_0550.txt%") )
            .select("path").rdd.map(r => r(0)).collect()
if (CAR_files.size > 0){
//loop through each CAR file -> read, process, write
  for(fileArray <- CAR_files) {
    try{
    writeToSQL(processFilesCAR(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("CAR daily File not available " )
}

// COMMAND ----------

import SqlWriter._
def processFilesHAY(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 
  println(file)
  sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter",",").option("ignoreLeadingWhiteSpace",true)   
     .load(file)
  .createOrReplaceTempView("facility_xls")

 //val loc = file.substring(15,18) 
 val qry1 = s"""select 
          'HAY' as  Location
         ,"HAMC ED" PlaceOfService
          ,`MRN` MRN
          ,string(`Hosp Act`)  VisitID
          ,'' ProviderID
          , ifnull(`Physician Name`,NULL) ProviderName
          , initcap(lower(`Last Name`)) PatientLastName
          , initcap(lower(`First Name`)) PatientFirstName
          , to_timestamp(replace('DOB',',',''),'MMM dd yyyy')  PatientDOB
          , SubString(`Gender`,1,1) PatientGender
          , `Disc Dispo` Disposition
          , ifnull(`Arrival DateTime`,NULL) ArrivalDate  
          ,  ifnull(`Arrival DateTime`,NULL) ArrivalDateTime
          , ifnull(`Triage DateTime`,NULL)  TriageDateTime
          ,  ifnull(`Bed DateTime`,NULL) BedDateTime
          ,  ifnull(`Provider DateTime`,NULL)  ProviderDateTime
          ,  ifnull(`Disposition DateTime`,NULL)  DispositionDateTime
          ,  ifnull(`Disch DateTime`,NULL) DepartureDateTime
          , `Chief Complaint` ChiefComplaint
          , '' AdmitFloor
          ,  string(`Acuity`) TriageLevel
          ,'' Nurse
          ,'' CustCol01
                    ,'' CustCol02
                    ,'' CustCol03
                    ,'' CustCol04
                    ,'' CustCol05
                    ,'' CustCol06
          ,current_timestamp() InsertedDate
          ,'${file}' FileName
      from 
      `facility_xls`
      where `MRN` IS NOT NULL AND `Hosp Act` IS NOT NULL  ;"""

spark.sql(qry1)

}


// COMMAND ----------


val HAY_files = df.filter($"path".like("%/mnt/troughput/archive/HAY%") )
            .select("path").rdd.map(r => r(0)).collect()
if (HAY_files.size > 0){
//loop through each HAY file -> read, process, write
  for(fileArray <- HAY_files) {
    try{
    writeToSQL(processFilesHAY(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("HAY daily File not available " )
}





// COMMAND ----------

import SqlWriter._
def processFilesDCH(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 
  println(file)
  val DCH_df=sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter","|")    
     .load(file)
  .createOrReplaceTempView("facility_xls")
 // DCH_df.show()
 // display(DCH_df)
 // DCH_df.printSchema
  
 // val data_table = "facility_xls"
//  DCH_df.createOrReplaceTempView(data_table)
 
 //val loc = file.substring(15,18)  
 val qry1 = s"""select 
          'DCH' as  Location
          ,'ED' PlaceOfService
          ,String(`MRN`) MRN
          ,String(`VisitID`)  VisitID
          ,`EDProviderNPIID` ProviderID
          , replace(Case When isnull(`MidLevelProviderName`) Then `EdProviderName` Else `MidLevelProviderName` End,',',' ') ProviderName
          , initcap(lower(substring(`PatientName`,1,instr(`PatientName`,',')-1))) PatientLastName
          , initcap(lower(substring(`PatientName`,instr(`PatientName`,',')+1,30))) PatientFirstName
          , `PatientDOB` PatientDOB
          ,`Gender` PatientGender
          ,`DischargeDisposition` Disposition
          , ifnull(to_date(`DateOfArrival`),NULL) ArrivalDate
          , ifnull(`DateOfArrival`,NULL) ArrivalDateTime
          , ifnull(`DateofTriage`,NULL) TriageDateTime
          , ifnull(Case When `DateTimeSeenByProvider` < `Bed` then `DateTimeSeenByProvider` else `Bed` End,NULL) BedDateTime
          , ifnull(`DateTimeSeenByProvider`,NULL) ProviderDateTime
          , ifnull(`DispositionDecisionTime`,NULL) DispositionDateTime
          , ifnull(`ExitDateTime`,NULL) DepartureDateTime
          , `ChiefComplaint` ChiefComplaint 
          , `AdmitFloorID` AdmitFloor
          , String(`TriageLevel`) TriageLevel
          ,'' Nurse
          ,'' CustCol01
                    ,'' CustCol02
                    ,'' CustCol03
                    ,'' CustCol04
                    ,'' CustCol05
                    ,'' CustCol06
           ,current_timestamp() InsertedDate
          ,'${file}' FileName
      from 
      `facility_xls`
      where `MRN` IS NOT NULL AND `VisitID` IS NOT NULL   ;""".stripMargin
spark.sql(qry1)
}

// COMMAND ----------

val DCH_files = df.filter($"path".like("%/mnt/troughput/archive/DCH%") )
            .select("path").rdd.map(r => r(0)).collect()
if (DCH_files.size > 0){
//loop through each DCH file -> read, process, write
  for(fileArray <- DCH_files) {
    try{
    writeToSQL(processFilesDCH(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("DCH daily File not available " )
}


// COMMAND ----------

import SqlWriter._
def processFilesPRI(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 
  println(file)

  val PRI_df=sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter",",").option("ignoreLeadingWhiteSpace",true) 
     .load(file)
  .createOrReplaceTempView("facility_xls")

 //val loc = file.substring(14,17)  
//  val loc = file.substring(15,18) 
 val qry1 = s"""select 
          'PRI' as  Location
          ,`Arrival Dept` PlaceOfService
          ,String(`MRN`) MRN
          ,String(`Hosp Act`)  VisitID
          ,'' ProviderID
          , ifnull(`APP Name`,`Physician Name`) ProviderName
          , initcap(lower(`Last Name`)) PatientLastName
          , initcap(lower(`First Name`)) PatientFirstName
          , concat(substring(`DOB`,1,4),'-',substring(`DOB`,5,2),'-',substring(`DOB`,7,2)) PatientDOB
          ,`Gender` PatientGender
          ,`Disc Dispo` Disposition
          , concat(substring(`Arrival Date`,1,4),'-',substring(`Arrival Date`,5,2),'-',substring(`Arrival Date`,7,2)) ArrivalDate
          , ifnull(`Arrival DateTime`,NULL) ArrivalDateTime
          
          , ifnull(`Triage DateTime`,NULL) TriageDateTime
          , ifnull(`Bed DateTime`,NULL) BedDateTime
          , ifnull(`Provider DateTime`,NULL) ProviderDateTime
          , ifnull(`Disposition DateTime`,NULL) DispositionDateTime
          , ifnull(`Admit DateTime`,`Disch DateTime`) DepartureDateTime
          , `Chief Complaint` ChiefComplaint 
          , '' AdmitFloor
          , string(`Acuity`) TriageLevel
          , '' Nurse
          ,'' CustCol01
                    ,'' CustCol02
                    ,'' CustCol03
                    ,'' CustCol04
                    ,'' CustCol05
                    ,'' CustCol06
          ,current_timestamp() InsertedDate
           ,'${file}' FileName
      from 
      `facility_xls`
      where `MRN` IS NOT NULL AND `Hosp Act` IS NOT NULL  ;""".stripMargin

spark.sql(qry1)

}


// COMMAND ----------

val PRI_files = df.filter($"path".like("%/mnt/troughput/archive/PRI%") )
//  val PRI_files = df.filter($"path".like("%/mnt/troughput/archive/PRI%") )
            .select("path").rdd.map(r => r(0)).collect()
if (PRI_files.size > 0){
//loop through each PRI file -> read, process, write
  for(fileArray <- PRI_files) {
    try{
    writeToSQL(processFilesPRI(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("PRI daily File not available " )
}


// COMMAND ----------

import SqlWriter._
def processFilesCRM(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 
  println(file)
  val loc = file.substring(15,18) 
  val CRM_df=sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter",",").option("quote", "\"")    
     .load(file)
  .createOrReplaceTempView("facility_xls")

  
 val qry1 = s"""select 
          'CRM' as  Location
          ,'ED' PlaceOfService
          ,String(`HOSPITAL_MEDICAL_RECORD_NUMBER`) MRN
          ,String(`HOSPITAL_ENCOUNTER_NUMBER`)  VisitID
          , String(`LAST_PHYS_DISP_NPI`) ProviderID
          , `LAST_PHYS_DISP_NM` ProviderName
          , '' PatientLastName
          , '' PatientFirstName
          , '1900-01-01' PatientDOB
          , '' PatientGender
          ,`ED_DISPOSITION` Disposition
         ,  ifnull(`ARRIVAL_DATE_DTTM`,NULL) ArrivalDate
          , ifnull(`ARRIVAL_DATE_DTTM`,NULL) ArrivalDateTime 
          , ifnull(`TRIAGE_DTTM`,NULL) TriageDateTime
          , ifnull(`BEDDED_DTTM`,NULL) BedDateTime
          , ifnull(`FIRST_PROV_DTTM`,NULL) ProviderDateTime
          , ifnull(`DISPOSITION_DTTM`,NULL) DispositionDateTime
          , ifnull(`ED_DEPART`,NULL) DepartureDateTime
          , '' ChiefComplaint 
          , '' AdmitFloor
          , `ESI_LEVEL` TriageLevel
          , '' Nurse
          ,'' CustCol01
          ,'' CustCol02
          ,'' CustCol03
          ,'' CustCol04
          ,'' CustCol05
          ,'' CustCol06
          ,current_timestamp() InsertedDate
          ,'${file}' FileName
      from 
      `facility_xls`
      where `HOSPITAL_MEDICAL_RECORD_NUMBER` IS NOT NULL AND `HOSPITAL_ENCOUNTER_NUMBER` IS NOT NULL  ;""".stripMargin

spark.sql(qry1)
}


// COMMAND ----------


val CRM_files = df.filter($"path".like("%/mnt/troughput/archive/CRM%") )
            .select("path").rdd.map(r => r(0)).collect()
if (CRM_files.size > 0){
//loop through each CRM file -> read, process, write
  for(fileArray <- CRM_files) {
    try{
    writeToSQL(processFilesCRM(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("CRM daily File not available " )
}

// COMMAND ----------

import SqlWriter._
def processFilesJEF(path: String): DataFrame = {
//no header   
  val file =  path.replace("dbfs:", "") 
  println(file)
  val loc = file.substring(15,18) 
  val JEF_df=sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter","|")    
     .load(file)
  .createOrReplaceTempView("facility_xls")

  
 val qry1 = s"""select 
          'JEF' as  Location
          ,'ED' PlaceOfService
          ,String(`MRN`) MRN
          ,String(`CSN`)  VisitID
          ,`PHYSICIAN/NPI` ProviderID
          , ifnull(`DOCTOR EXTENDER NAME`,`PHYSICIAN NAME`) ProviderName 
          , initcap(lower(`PAT LAST NAME`)) PatientLastName
          , initcap(lower(`PAT FIRST NAME`)) PatientFirstName
          , `DOB` PatientDOB
          ,`SEX` PatientGender
          ,`DISPO` Disposition
          , ifnull(`DATE`,NULL) ArrivalDate
          , ifnull(`ARRIVAL`,NULL) ArrivalDateTime        
          , ifnull(`TRIAGE`,NULL) TriageDateTime
          , ifnull(`Bed`,NULL) BedDateTime
          , ifnull(`PROVIDER`,NULL) ProviderDateTime
          ,  ifnull(`DISPOSITION`,NULL) DispositionDateTime
          , ifnull(`EXIT`,NULL) DepartureDateTime
          , `CHIEF COMPLAINT` ChiefComplaint 
          , '' AdmitFloor
          , `TRIAGE LEVEL` TriageLevel
          , '' Nurse
          ,'' CustCol01
                    ,'' CustCol02
                    ,'' CustCol03
                    ,'' CustCol04
                    ,'' CustCol05
                    ,'' CustCol06
          ,current_timestamp() InsertedDate
          ,'${file}' FileName
      from 
      `facility_xls`
      where `MRN` IS NOT NULL AND `CSN` IS NOT NULL  ;""".stripMargin
spark.sql(qry1)

}

// COMMAND ----------


val JEF_files = df.filter($"path".like("%/mnt/troughput/archive/JEF%") )
            .select("path").rdd.map(r => r(0)).collect()
if (JEF_files.size > 0){
//loop through each JEF file -> read, process, write
  for(fileArray <- JEF_files) {
    try{
    writeToSQL(processFilesJEF(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("JEF daily File not available " )
}



// COMMAND ----------

import SqlWriter._
def processFilesVHC(path: String): DataFrame = {
//no header   
  val file =  path.replace("dbfs:", "") 
  println(file)
  val loc = file.substring(15,18) 
  val JEF_df=sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter","|")    
     .load(file)
  .createOrReplaceTempView("facility_xls")

  
 val qry1 = s"""select 
          'VHC' as  Location
          ,'ED' PlaceOfService
          ,String(`MRN`) MRN
          ,String(`CSN`)  VisitID
          ,`First ED Provider` ProviderID
          , `First ED Provider` ProviderName 
          , initcap(lower(`Last Name`)) PatientLastName
          , initcap(lower(`First Name`)) PatientFirstName          
          , `Birth Date` PatientDOB
          ,`Gender` PatientGender
          ,`ED Disch Disposition` Disposition 
          , ifnull(`Arrv Date`,NULL) ArrivalDate
          ,concat(substr(trim(`Arrv Date/Time`),1,11),substr(trim(`Arrv Date/Time`),instr(trim(`Arrv Date/Time`),' ')+1,2),':',substr(trim(`Arrv Date/Time`),instr(trim(`Arrv Date/Time`),' ')+3,2)) ArrivalDateTime
          ,concat(substr(trim(`Triage Start`),1,instr(trim(`Triage Start`),'/')-1),'/',substr(trim(`Triage Start`),instr(trim(`Triage Start`),'/')+1,instr(trim(`Triage Start`),' ')-3),'/','2021',' ',substr(trim(`Triage Start`),instr(trim(`Triage Start`),' ')+1,2),':',substr(trim(`Triage Start`),instr(trim(`Triage Start`),' ')+3,2)) TriageDateTime
          ,concat(substr(trim(`Roomed`),1,instr(trim(`Roomed`),'/')-1),'/',substr(trim(`Roomed`),instr(trim(`Roomed`),'/')+1,instr(trim(`Roomed`),' ')-3),'/','2021',' ',substr(trim(`Roomed`),instr(trim(`Roomed`),' ')+1,2),':',substr(trim(`Roomed`),instr(trim(`Roomed`),' ')+3,2)) BedDateTime
          , ifnull(`Provider Seen`,NULL) ProviderDateTime
           ,concat(substr(trim(`Dispo`),1,instr(trim(`Dispo`),'/')-1),'/',substr(trim(`Dispo`),instr(trim(`Dispo`),'/')+1,instr(trim(`Dispo`),' ')-3),' ',substr(trim(`Dispo`),instr(trim(`Dispo`),' ')+1,2),':',substr(trim(`Dispo`),instr(trim(`Dispo`),' ')+3,2)) DispositionDateTime
          ,concat(substr(trim(`Departed`),1,instr(trim(`Departed`),'/')-1),'/',substr(trim(`Departed`),instr(trim(`Departed`),'/')+1,instr(trim(`Departed`),' ')-3),'/','2021',' ',substr(trim(`Departed`),instr(trim(`Departed`),' ')+1,2),':',substr(trim(`Departed`),instr(trim(`Departed`),' ')+3,2)) DepartureDateTime
          , `Chief Complaint` ChiefComplaint 
          , '' AdmitFloor
          , `Acuity` TriageLevel
          , '' Nurse
          ,'' CustCol01
                    ,'' CustCol02
                    ,'' CustCol03
                    ,'' CustCol04
                    ,'' CustCol05
                    ,'' CustCol06
          ,current_timestamp() InsertedDate
          ,'${file}' FileName
      from 
      `facility_xls`
      where `MRN` IS NOT NULL AND `CSN` IS NOT NULL  ;""".stripMargin
spark.sql(qry1)

}

// COMMAND ----------

// MAGIC %sql
// MAGIC  select 
// MAGIC           'VHC' as  Location
// MAGIC           ,'ED' PlaceOfService
// MAGIC           ,String(`MRN`) MRN
// MAGIC           ,String(`CSN`)  VisitID
// MAGIC           ,`First ED Provider` ProviderID
// MAGIC           , `First ED Provider` ProviderName 
// MAGIC           , initcap(lower(`Last Name`)) PatientLastName
// MAGIC           , initcap(lower(`First Name`)) PatientFirstName          
// MAGIC           , `Birth Date` PatientDOB
// MAGIC           ,`Gender` PatientGender
// MAGIC           ,`ED Disch Disposition` Disposition 
// MAGIC           , ifnull(`Arrv Date`,NULL) ArrivalDate
// MAGIC           ,concat(substr(trim(`Arrv Date/Time`),1,11),substr(trim(`Arrv Date/Time`),instr(trim(`Arrv Date/Time`),' ')+1,2),':',substr(trim(`Arrv Date/Time`),instr(trim(`Arrv Date/Time`),' ')+3,2)) ArrivalDateTime
// MAGIC           ,concat(substr(trim(`Triage Start`),1,instr(trim(`Triage Start`),'/')-1),'/',substr(trim(`Triage Start`),instr(trim(`Triage Start`),'/')+1,instr(trim(`Triage Start`),' ')-3),'/','2021',' ',substr(trim(`Triage Start`),instr(trim(`Triage Start`),' ')+1,2),':',substr(trim(`Triage Start`),instr(trim(`Triage Start`),' ')+3,2)) TriageDateTime
// MAGIC           ,concat(substr(trim(`Roomed`),1,instr(trim(`Roomed`),'/')-1),'/',substr(trim(`Roomed`),instr(trim(`Roomed`),'/')+1,instr(trim(`Roomed`),' ')-3),'/','2021',' ',substr(trim(`Roomed`),instr(trim(`Roomed`),' ')+1,2),':',substr(trim(`Roomed`),instr(trim(`Roomed`),' ')+3,2)) BedDateTime
// MAGIC           , ifnull(`Provider Seen`,NULL) ProviderDateTime
// MAGIC            ,concat(substr(trim(`Dispo`),1,instr(trim(`Dispo`),'/')-1),'/',substr(trim(`Dispo`),instr(trim(`Dispo`),'/')+1,instr(trim(`Dispo`),' ')-3),' ',substr(trim(`Dispo`),instr(trim(`Dispo`),' ')+1,2),':',substr(trim(`Dispo`),instr(trim(`Dispo`),' ')+3,2)) DispositionDateTime
// MAGIC           ,concat(substr(trim(`Departed`),1,instr(trim(`Departed`),'/')-1),'/',substr(trim(`Departed`),instr(trim(`Departed`),'/')+1,instr(trim(`Departed`),' ')-3),'/','2021',' ',substr(trim(`Departed`),instr(trim(`Departed`),' ')+1,2),':',substr(trim(`Departed`),instr(trim(`Departed`),' ')+3,2)) DepartureDateTime
// MAGIC           , `Chief Complaint` ChiefComplaint 
// MAGIC           , '' AdmitFloor
// MAGIC           , `Acuity` TriageLevel
// MAGIC           , '' Nurse
// MAGIC           ,'' CustCol01
// MAGIC                     ,'' CustCol02
// MAGIC                     ,'' CustCol03
// MAGIC                     ,'' CustCol04
// MAGIC                     ,'' CustCol05
// MAGIC                     ,'' CustCol06
// MAGIC           ,current_timestamp() InsertedDate 
// MAGIC         
// MAGIC       from 
// MAGIC       `facility_xls`

// COMMAND ----------


val VHC_files = df.filter($"path".like("%/mnt/troughput/archive/VHC%") )
            .select("path").rdd.map(r => r(0)).collect()
if (VHC_files.size > 0){
//loop through each VHC file -> read, process, write
  for(fileArray <- VHC_files) {
    try{
    writeToSQL(processFilesVHC(fileArray.toString), "pre_stg", "Throughput_Test",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("VHC daily File not available " )
}

// COMMAND ----------

import SqlWriter._
def processFilesCPR(path: String): DataFrame = {
//no header  
  val file =  path.replace("dbfs:", "") 
  println(file)
  val loc = file.substring(15,18) 
  import org.apache.spark.sql.types._
   val schema = StructType(
  List(
    StructField("Location", StringType, true),
    StructField("PlaceOfService", StringType, true),
     StructField("MRN", StringType, true),
    StructField("Visit", StringType, true),
     StructField("PhysicianNPI", StringType, true),
     StructField("PhysicianName", StringType, true),
    StructField("MLP_NPI", StringType, true),
     StructField("MLPName", StringType, true),
    StructField("PatientLastname", StringType, true),
    StructField("PatientFirstname", StringType, true),
     StructField("PatientDOB", StringType, true),
    StructField("PatientGender", StringType, true),
     StructField("DispositionCode", StringType, true),
    StructField("Disposition", StringType, true),
     StructField("Date", StringType, true),
     StructField("Arrival", StringType, true),
    StructField("Triage", StringType, true),
     StructField("Bed", StringType, true),
    StructField("Provider", StringType, true),
     StructField("Dispo", StringType, true),
    StructField("Exit", StringType, true),
    StructField("Exit2", StringType, true),
     StructField("ChiefComplaint", StringType, true),
    StructField("AdmitFloor", StringType, true),
     StructField("AdmitFloorDesc", StringType, true),
    StructField("TriageLevel", StringType, true),
     StructField("NurseID", StringType, true),
    StructField("Nurse", StringType, true)
    )
  )
  sqlContext.read.format("csv").option("header", "false").option("inferSchema", "true").option("delimiter","|").schema(schema)    
     .load(file)
  .createOrReplaceTempView("facility_xls")

 val qry1 = s"""select 
          'CPR' as  Location
          ,`PlaceOfService` PlaceOfService
          ,String(`MRN`) MRN
          ,String(`VISIT`)  VisitID
          ,String(`PhysicianNPI`) ProviderID
          , `PhysicianName` ProviderName 
          , initcap(lower(`PatientLastname`)) PatientLastName
          , initcap(lower(`PatientFirstname`)) PatientFirstName
          , `PatientDOB` PatientDOB
          ,`PatientGender` PatientGender
          ,`DispositionCode` Disposition
          ,  `DATE` ArrivalDate
          , ifnull(case when `ARRIVAL` = ':' then null else `ARRIVAL` end ,NULL) ArrivalDateTime        
          , ifnull(case when `TRIAGE` = ':' then null else `TRIAGE` end ,NULL) TriageDateTime
          , ifnull(case when `Bed` = ':' then null else `Bed` end ,NULL) BedDateTime
          , ifnull(case when `PROVIDER` = ':' then null else `PROVIDER` end ,NULL) ProviderDateTime
          ,  ifnull(case when `DISPO`  = ':' then null else `DISPO` end ,NULL) DispositionDateTime
          , ifnull(to_timestamp(concat(`Exit`,' ',`Exit2`)),NULL) DepartureDateTime
          , `ChiefComplaint` ChiefComplaint 
          , String(`AdmitFloor`) AdmitFloor
          , String(`TriageLevel`) TriageLevel
          , '' Nurse
          ,'' CustCol01
                    ,'' CustCol02
                    ,'' CustCol03
                    ,'' CustCol04
                    ,'' CustCol05
                    ,'' CustCol06
          ,current_timestamp() InsertedDate
          ,'${file}' FileName
      from 
      `facility_xls`
      where `MRN` IS NOT NULL AND `VISIT` IS NOT NULL  ;""".stripMargin
spark.sql(qry1)
}




// COMMAND ----------


val CPR_files = df.filter($"path".like("%/mnt/troughput/archive/CPR%") )
            .select("path").rdd.map(r => r(0)).collect()
if (CPR_files.size > 0){
//loop through each CPR file -> read, process, write
  for(fileArray <- CPR_files) {
    try{
    writeToSQL(processFilesCPR(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("CPR daily File not available " )
}


// COMMAND ----------

import SqlWriter._
def processFilesSIB(path: String): DataFrame = {
//no header  
  val file =  path.replace("dbfs:", "") 
  println(file)
  val loc = file.substring(15,18) 
  import org.apache.spark.sql.types._
   val schema = StructType(
  List(
    StructField("Location", StringType, true),
    StructField("PlaceOfService", StringType, true),
     StructField("MRN", StringType, true),
    StructField("Visit", StringType, true),
     StructField("PhysicianNPI", StringType, true),
     StructField("PhysicianName", StringType, true),
    StructField("MLP_NPI", StringType, true),
     StructField("MLPName", StringType, true),
    StructField("PatientLastname", StringType, true),
    StructField("PatientFirstname", StringType, true),
     StructField("PatientDOB", StringType, true),
    StructField("PatientGender", StringType, true),
     StructField("DispositionCode", StringType, true),
    StructField("Disposition", StringType, true),
     StructField("Arrival", StringType, true),
    StructField("Triage", StringType, true),
     StructField("BedDateTime", StringType, true),
    StructField("Provider", StringType, true),
     StructField("Dispo", StringType, true),
    StructField("Exit", StringType, true),
     StructField("ChiefComplaint", StringType, true),
    StructField("AdmitFloor", StringType, true),
     StructField("AdmitFloorDesc", StringType, true),
    StructField("TriageLevel", StringType, true),
     StructField("NurseID", StringType, true),
    StructField("Nurse", StringType, true)
    )
  )
  sqlContext.read.format("csv").option("header", "false").option("inferSchema", "false").option("delimiter","|").schema(schema)    
     .load(file)
  .createOrReplaceTempView("facility_xls")
  
  
val qry1 = s"""select 
          'SIB' as  Location
          ,`PlaceOfService` PlaceOfService
          ,String(`MRN`) MRN
          ,String(`Visit`)  VisitID
          ,String(`PhysicianNPI`) ProviderID
          , `PhysicianName` ProviderName 
          , initcap(lower(`PatientLastname`)) PatientLastName
          , initcap(lower(`PatientFirstname`)) PatientFirstName
          , `PatientDOB` PatientDOB
          ,`PatientGender` PatientGender
          ,`DispositionCode` Disposition
          , `Arrival` ArrivalDate
          , ifnull(`Arrival`,NULL) ArrivalDateTime        
          , ifnull(`Triage`,NULL) TriageDateTime
          , ifnull(`BedDateTime`,NULL) BedDateTime
          , ifnull(`Provider`,NULL) ProviderDateTime
          ,  ifnull(`Dispo`,NULL) DispositionDateTime
          , ifnull(`Exit`,NULL) DepartureDateTime
          , `ChiefComplaint` ChiefComplaint 
          , String(`AdmitFloor`) AdmitFloor
          , String(`TriageLevel`) TriageLevel
          , '' Nurse
          ,'' CustCol01
          ,'' CustCol02
          ,'' CustCol03
          ,'' CustCol04
          ,'' CustCol05
          ,'' CustCol06
          ,current_timestamp() InsertedDate
          ,'${file}' FileName
      from 
      `facility_xls`;""".stripMargin
 spark.sql(qry1)

}

// COMMAND ----------


val SIB_files = df.filter($"path".like("%/mnt/troughput/archive/SIB%") )
            .select("path").rdd.map(r => r(0)).collect()
if (SIB_files.size > 0){
//loop through each SIB file -> read, process, write
  for(fileArray <- SIB_files) {
    try{
    writeToSQL(processFilesSIB(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("SIB daily File not available " )
}




// COMMAND ----------


  
import SqlWriter._
def processFilesSTO(path: String): DataFrame = {
//no header   
  val file =  path.replace("dbfs:", "") 
  println(file)
  val loc = file.substring(15,18) 
  val STO_df=sqlContext.read.format("csv").option("header", "false").option("inferSchema", "false").option("delimiter","|")    
     .load(file)
  //RHC_df.show()
  //display(RHC_df)
  //RHC_df.printSchema
  
  val data_table = "facility_xls"
  STO_df.createOrReplaceTempView(data_table)
  

  
val qry1 = s"""select 
          'STO' as  Location
          ,substr(`_c0`,78,4)  PlaceOfService
          , substr(`_c0`,1,11) MRN
          ,substr(`_c0`,12,13)  VisitID
          ,'' ProviderID
         , substr(`_c0`,26,11) ProviderName 
         , substr(substr(`_c0`,38,25), 1, instr(substr(`_c0`,38,25), ',')-1)  PatientLastName

         ,substr(substr(`_c0`,38,25), instr(substr(`_c0`,38,25), ',') +1 ,25) PatientFirstName
         ,concat(substr(`_c0`,63,2) , '-' , substring(`_c0`,66,2) , '-' , case when substr(`_c0`,69,2) > '21' then  concat('19',substr(`_c0`,69,2) ) else concat('20',substr(`_c0`,69,2)) end)  PatientDOB
         ,substr(`_c0`,72,1) PatientGender
         ,substr(`_c0`,74,4) Disposition
         , concat(substr(`_c0`,83,2) , '-' , substring(`_c0`,86,2) , '-' , concat('20',substr(`_c0`,89,2))) ArrivalDate
         ,concat('20',substr(`_c0`,89,2 ),'-',substr(`_c0`,83,2) , '-' , substring(`_c0`,86,2),' ',substr(`_c0`,92,2 ),':',substr(`_c0`,94,2 ),':00') ArrivalDateTime        
        , case when trim(substr(`_c0`,98,8)) > ''   then concat('20',substr(`_c0`,104,2 ),'-',substr(`_c0`,98,2) , '-' , substring(`_c0`,101,2),' ',substr(`_c0`,107,2 ),':',substr(`_c0`,109,2 ),':00') else NULL end TriageDateTime
       ,case when trim(substr(`_c0`,112,15)) > ''   then concat('20',substr(`_c0`,118,2 ),'-',substr(`_c0`,112,2) , '-' , substring(`_c0`,115,2),' ',substr(`_c0`,121,2 ),':',substr(`_c0`,123,2 ),':00') else NULL end BedDateTime
       ,case when trim(substr(`_c0`,127,15)) > ''   then concat('20',substr(`_c0`,133,2 ),'-',substr(`_c0`,127,2) , '-' , substring(`_c0`,130,2),' ',substr(`_c0`,136,2 ),':',substr(`_c0`,138,2 ),':00') else NULL end ProviderDateTime
       , case when substr(`_c0`,143,2) > '' then NULL else '' end  DispositionDateTime
       ,case when trim(substr(`_c0`,157,15)) > ''   then concat('20',substr(`_c0`,163,2 ),'-',substr(`_c0`,157,2) , '-' , substring(`_c0`,160,2),' ',substr(`_c0`,166,2 ),':',substr(`_c0`,168,2 ),':00') else NULL end  DepartureDateTime
        ,substr(`_c0`,178,17 ) ChiefComplaint  
        ,substr(`_c0`,172,5 ) AdmitFloor
       ,'' TriageLevel
       ,'' Nurse
                 ,'' CustCol01
                    ,'' CustCol02
                    ,'' CustCol03
                    ,'' CustCol04
                    ,'' CustCol05
                    ,'' CustCol06
       ,current_timestamp()  InsertedDate
       ,'${file}' FileName
      from 
      `facility_xls`
      where  substr(`_c0`,1,11) IS NOT NULL AND substr(`_c0`,12,13) IS NOT NULL and substr(`_c0`,89,2) > '14' ;"""
  println(qry1)
spark.sql(qry1)
}




// COMMAND ----------

val STO_files = df.filter($"path".like("%/mnt/troughput/archive/STO%")  )
            .select("path").rdd.map(r => r(0)).collect()
if (STO_files.size > 0){
//loop through each ALX file -> read, process, write
  for(fileArray <- STO_files) {
    try{
    writeToSQL(processFilesSTO(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("STO daily File not available " )
}

// COMMAND ----------

import SqlWriter._
def processFilesRHC(path: String): DataFrame = {
//no header   
  val file =  path.replace("dbfs:", "") 
  println(file)
  val loc = file.substring(15,18) 
  val RHC_df=sqlContext.read.format("csv").option("header", "false").option("inferSchema", "false").option("delimiter","|")    
     .load(file)
  //RHC_df.show()
  //display(RHC_df)
  //RHC_df.printSchema
  
  val data_table = "facility_xls"
  RHC_df.createOrReplaceTempView(data_table)
  val qry1 = s"""select 
          'RHC' as  Location
          ,substr(`_c0`,78,4)  PlaceOfService
          , substr(`_c0`,1,11) MRN
          ,substr(`_c0`,12,13)  VisitID
          ,'' ProviderID
         , substr(`_c0`,26,11) ProviderName 
         , substr(substr(`_c0`,38,25), 1, instr(substr(`_c0`,38,25), ',')-1)  PatientLastName

         ,substr(substr(`_c0`,38,25), instr(substr(`_c0`,38,25), ',') +1 ,25) PatientFirstName
         ,concat(substr(`_c0`,63,2) , '-' , substring(`_c0`,66,2) , '-' , case when substr(`_c0`,69,2) > '21' then  concat('19',substr(`_c0`,69,2) ) else concat('20',substr(`_c0`,69,2)) end)  PatientDOB
         ,substr(`_c0`,72,1) PatientGender
         ,substr(`_c0`,74,4) Disposition
         , concat(substr(`_c0`,83,2) , '-' , substring(`_c0`,86,2) , '-' , concat('20',substr(`_c0`,89,2))) ArrivalDate
         ,concat('20',substr(`_c0`,89,2 ),'-',substr(`_c0`,83,2) , '-' , substring(`_c0`,86,2),' ',substr(`_c0`,92,2 ),':',substr(`_c0`,94,2 ),':00') ArrivalDateTime        
        , case when trim(substr(`_c0`,98,8)) > ''   then concat('20',substr(`_c0`,104,2 ),'-',substr(`_c0`,98,2) , '-' , substring(`_c0`,101,2),' ',substr(`_c0`,107,2 ),':',substr(`_c0`,109,2 ),':00') else NULL end TriageDateTime
       ,case when trim(substr(`_c0`,112,15)) > ''   then concat('20',substr(`_c0`,118,2 ),'-',substr(`_c0`,112,2) , '-' , substring(`_c0`,115,2),' ',substr(`_c0`,121,2 ),':',substr(`_c0`,123,2 ),':00') else NULL end BedDateTime
       ,case when trim(substr(`_c0`,127,15)) > ''   then concat('20',substr(`_c0`,133,2 ),'-',substr(`_c0`,127,2) , '-' , substring(`_c0`,130,2),' ',substr(`_c0`,136,2 ),':',substr(`_c0`,138,2 ),':00') else NULL end ProviderDateTime
       , case when substr(`_c0`,143,2) > '' then NULL else '' end  DispositionDateTime
       ,case when trim(substr(`_c0`,157,15)) > ''   then concat('20',substr(`_c0`,163,2 ),'-',substr(`_c0`,157,2) , '-' , substring(`_c0`,160,2),' ',substr(`_c0`,166,2 ),':',substr(`_c0`,168,2 ),':00') else NULL end  DepartureDateTime
        ,substr(`_c0`,178,17 ) ChiefComplaint  
        ,substr(`_c0`,172,5 ) AdmitFloor
       ,'' TriageLevel
       ,'' Nurse
                 ,'' CustCol01
                    ,'' CustCol02
                    ,'' CustCol03
                    ,'' CustCol04
                    ,'' CustCol05
                    ,'' CustCol06
       ,current_timestamp()  InsertedDate
       ,'${file}' FileName
      from 
      `facility_xls`
      where  substr(`_c0`,1,11) IS NOT NULL AND substr(`_c0`,12,13) IS NOT NULL and substr(`_c0`,89,2) > '14' ;"""
  println(qry1)
spark.sql(qry1)

}



// COMMAND ----------

val RHC_files = df.filter($"path".like("%/mnt/troughput/archive/RHC%")  )
            .select("path").rdd.map(r => r(0)).collect()
if (RHC_files.size > 0){
//loop through each ALX file -> read, process, write
  for(fileArray <- RHC_files) {
    try{
    writeToSQL(processFilesRHC(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("RHC daily File not available " )
}

// COMMAND ----------

//IMMH sites


import SqlWriter._
def processFilesUH(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 
  println(file)
  val loc = file.substring(28,34) 
  val ProvID = ""
  val DOB = ""
  val Gender = ""
  val UH_df = spark.read.format("com.crealytics.spark.excel")
  .option("header", "true")
  .option("treatEmptyValuesAsNulls", "false")
  .option("inferSchema", "false")
  .option("addColorColumns", "false")
  .option("dataAddress", "default") 
  .load(file)
  .createOrReplaceTempView("facility_xls")
  //display(UES_df)
 val qry1 = s"""select 
          '${loc}' as  Location
          ,'ED' PlaceOfService
          ,String(`MRN`) MRN
          ,String(`Encounter`)  VisitID
          , case when '${ProvID}' = '' then Null else '${ProvID}' end  ProviderID
          , ifnull(ifnull(`First_NP_Assigned`,`First_PA_Assigned`),`First_Attending_Assigned`) ProviderName 
          , initcap(lower(substring(`Patient_Name`,1,instr(`Patient_Name`,',')-1))) PatientLastName
          , initcap(lower(substring(`Patient_Name`,instr(`Patient_Name`,',')+1,30))) PatientFirstName
          , case when '${DOB}' = '' then Null else '${DOB}' end PatientDOB
          , case when '${Gender}' = '' then Null else '${DOB}' end PatientGender
          ,`Disposition_Status` Disposition
          , `Registration_DateTime` ArrivalDate
          , `Registration_DateTime` ArrivalDateTime        
          ,`Triage_DateTime` TriageDateTime
          , `Bed_Placement_Time` BedDateTime
          , `Time_First_Medical_Care_Provider_Assigned` ProviderDateTime
          ,  `Time_of_Last_Disposition_Decision` DispositionDateTime
          , `Disposition_Time` DepartureDateTime
          , substring(`ChiefComplaint`,1,200) ChiefComplaint 
          , '' AdmitFloor
          , String(`Triage_Score`) TriageLevel
          , '' Nurse
           ,'' CustCol01
           ,'' CustCol02
           ,'' CustCol03
           ,'' CustCol04
           ,'' CustCol05
           ,'' CustCol06
       ,current_timestamp()  InsertedDate
       ,'${file}' FileName
      from 
      `facility_xls`
        ;"""
   println(qry1)
spark.sql(qry1)
}


// COMMAND ----------

val UH_files = df.filter($"path".like("%/mnt/troughput/archive/UES%")    )
            .select("path").rdd.map(r => r(0)).collect()
if (UH_files.size > 0){
//loop through each ALX file -> read, process, write
  for(fileArray <- UH_files) {
    try{
    writeToSQL(processFilesUH(fileArray.toString), "pre_stg", "Throughput",  Seq("Location","VisitID","ArrivalDate"),"update-insert", Map.empty, jdbcUrl, jdbcConnectionProperties)
      dbutils.fs.mv(fileArray.toString, fileArray.toString.replace("incoming","archive"))
      
    }
    catch{ 
      case _: Throwable => println("Exception: " + fileArray.toString)
    }
    
  }
}else{
   println("UES daily File not available " )
}

// COMMAND ----------

println("Mercey Throughput")
/*
import SqlWriter._
def processFilesMercy(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 
  println(file)
  val loc = file.substring(28,34) 
  val ProvID = ""
  val DOB = ""
  val Gender = ""
  val UH_df = spark.read.format("com.crealytics.spark.excel")
  .option("header", "true")
  .option("treatEmptyValuesAsNulls", "false")
println("Mercey Throughput")
  /*.option("inferSchema", "false")
  .option("addColorColumns", "false")
  .option("dataAddress", "default") 
  .load(file)
  .createOrReplaceTempView("facility_xls")
  //display(UES_df)
 val qry1 = s"""select 
          '${loc}' as  Location
          ,'ED' PlaceOfService
          ,String(`MRN`) MRN
          ,String(`Hospital Account`)  VisitID
          , case when '${ProvID}' = '' then Null else '${ProvID}' end  ProviderID
          , ifnull(ifnull(`First_NP_Assigned`,`First_PA_Assigned`),`First_Attending_Assigned`) ProviderName 
          , '' PatientLastName
          , '' PatientFirstName
          , case when '${DOB}' = '' then Null else '${DOB}' end PatientDOB
          , case when '${Gender}' = '' then Null else '${DOB}' end PatientGender
          ,`ED Disposition` Disposition
          , `Arrv Date/Time` ArrivalDate
          , `Arrv Date/Time` ArrivalDateTime        
          ,current_timestamp() TriageDateTime
          , current_timestamp() BedDateTime
          , current_timestamp() ProviderDateTime
          ,  current_timestamp() DispositionDateTime
          , `Dprt Date/Time` DepartureDateTime
          , '' ChiefComplaint 
          , '' AdmitFloor
          , String(`Acuity`) TriageLevel
          , '' Nurse
           ,'' CustCol01
           ,'' CustCol02
           ,'' CustCol03
           ,'' CustCol04
           ,'' CustCol05
           ,'' CustCol06
       ,current_timestamp()  InsertedDate
       ,'${file}' FileName
      from 
      `facility_xls`
        ;"""
   println(qry1)
spark.sql(qry1)
}
*/

// COMMAND ----------

// COMMAND ----------
/*

// COMMAND ----------

def processFilesCRMC(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 
  println(file)
  val CRMC_df=sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter","|")    
     .load(file)
  CRMC_df.show()
  display(CRMC_df)
  CRMC_df.printSchema
  
  val data_table = "facility_xls"
  CRMC_df.createOrReplaceTempView(data_table)
  println(data_table)
}

// COMMAND ----------

def processFilesDecatur(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 
  println(file)
  val Decatur_df=sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter","|")    
     .load(file)
  Decatur_df.show()
  display(Decatur_df)
  Decatur_df.printSchema
  
  val data_table = "facility_xls"
  Decatur_df.createOrReplaceTempView(data_table)
  println(data_table)
}

// COMMAND ----------

def processFilesHardtner(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 
  println(file)
  val Hardtner_df=sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter","|")    
     .load(file)
  Hardtner_df.show()
  display(Hardtner_df)
  Hardtner_df.printSchema
  
  val data_table = "facility_xls"
  Hardtner_df.createOrReplaceTempView(data_table)
  println(data_table)
}

// COMMAND ----------

def processFilesHighlands(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 
  println(file)
  val Highlands_df=sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter","|")    
     .load(file)
  Highlands_df.show()
  display(Highlands_df)
  Highlands_df.printSchema
  
  val data_table = "facility_xls"
  Highlands_df.createOrReplaceTempView(data_table)
  println(data_table)
}

// COMMAND ----------

def processFilesIUF(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 
  println(file)
  val IUF_df=sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter","|")    
     .load(file)
  IUF_df.show()
  display(IUF_df)
  IUF_df.printSchema
  
  val data_table = "facility_xls"
  IUF_df.createOrReplaceTempView(data_table)
  println(data_table)
}

// COMMAND ----------

def processFilesLakeHuron(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 
  println(file)
  val LakeHuron_df=sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter","|")    
     .load(file)
  LakeHuron_df.show()
  display(LakeHuron_df)
  LakeHuron_df.printSchema
  
  val data_table = "facility_xls"
  LakeHuron_df.createOrReplaceTempView(data_table)
  println(data_table)
}

// COMMAND ----------

def processFilesMethodist(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 
  println(file)
  val Methodist_df=sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter","|")    
     .load(file)
  Methodist_df.show()
  display(Methodist_df)
  Methodist_df.printSchema
  
  val data_table = "facility_xls"
  Methodist_df.createOrReplaceTempView(data_table)
  println(data_table)
}

// COMMAND ----------

def processFilesPromedica(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 
  println(file)
  val Promedica_df=sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter","|")    
     .load(file)
  Promedica_df.show()
  display(Promedica_df)
  Promedica_df.printSchema
  
  val data_table = "facility_xls"
  Promedica_df.createOrReplaceTempView(data_table)
  println(data_table)
}

// COMMAND ----------

def processFilesStJohn(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 
  println(file)
  val StJohn_df=sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter","|")    
     .load(file)
  StJohn_df.show()
  display(StJohn_df)
  StJohn_df.printSchema
  
  val data_table = "facility_xls"
  StJohn_df.createOrReplaceTempView(data_table)
  println(data_table)
}

// COMMAND ----------

def processFilesVAN(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 
  println(file)
  val VAN_df=sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter","|")    
     .load(file)
  VAN_df.show()
  display(VAN_df)
  VAN_df.printSchema
  
  val data_table = "facility_xls"
  VAN_df.createOrReplaceTempView(data_table)
  println(data_table)
}


// COMMAND ----------

//IMM files
// COMMAND ----------




// COMMAND ----------

def processFilesBaptist(path: String): DataFrame = {
  
  val file =  path.replace("dbfs:", "") 
  println(file)
  val loc = "Baptist"
  val Baptist_df=sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter",",")    
     .load(file)
  Baptist_df.show()
  display(Baptist_df)
  Baptist_df.printSchema
  
  val data_table = "facility_xls"
  Baptist_df.createOrReplaceTempView(data_table)
  
 val qry1 = s"""select 
          '${loc}' as  Location
          ,"ED" PlaceOfService
          ,`MRN` MRN
          ,`Hospital Account`  VisitID
          , ifnull(ifnull(`NP NPI`,`PA NPI`),`MD NPI`) ProviderName 
          , '' PatientLastName
          , '' PatientFirstName
          , '' PatientDOB
          ,'' PatientGender
          ,`ED Disposition` Disposition
          , `Arrival` ArrivalDate
          , `Arrival` ArrivalDateTime        
          , `Triage Start` Triage
          , `Arrival to Room` BedDateTime
          , `Arrival to Room` ProviderDateTime
          ,  `Arrival to Dispo` DispositionDateTime
          , '' DepartureDateTime
          , '' ChiefComplaint 
          , '' AdmitFloor
          , `Acuity Abbr` TriageLevel
          , '' Nurse
          ,NULL InsertedDate
          , """" + file + """" FileName
      from 
      `facility_xls`
      where `MRN` IS NOT NULL AND `Hospital Account` IS NOT NULL  ;"""
  println(qry1)
  val results = spark.sql(qry1)
  println(results)
  

    import org.apache.spark.sql.SaveMode

    spark.sql("select * from xls_out")
    .write
    .mode(SaveMode.Append) // <--- Append to the existing table
    .jdbc(jdbcUrl, "PatientVisitStageBaptist", connectionProperties)
}

val df2 = df.filter($"path".like("%Baptist%"))
val myArray = df2.select("path").rdd.map(r => r(0)).collect()
for(myString <- myArray) {
    processFilesBaptist(myString.toString);
}

*/
println("IMM sites are working in progress")

// COMMAND ----------

// MAGIC %sql
// MAGIC select '01/18/2021 1703' ArrivalDate,
// MAGIC cast('01/18/2021 1703' as TIMESTAMP), 
// MAGIC cast('01/18/2021 1703' as timestamp) + interval 10 minutes as ArrivalPlusMinutes,
// MAGIC --to_timestamp('01/18/2021 1703','dd/MM/yyyy hhmm') 
// MAGIC to_timestamp("01/18/2021 1703","MM-dd-yyyy HHmm")
