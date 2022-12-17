import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._



object BcgCaseStudy extends App {

val inputData=args(0)
val outputData=args(1)

val spark = SparkSession.builder().master("yarn").appName("BcgCaseStudy").getOrCreate()
import spark.implicits._

def Analysis1(inputFileName : String,outputFileName : String): Unit = {
var df_analysis1 = spark.read.option("header",true).csv(inputData+"/"+inputFileName+".csv")
var df_analysis = df_analysis1.distinct()
val result1=df_analysis.filter(( $"DEATH_CNT">0) and ( $"PRSN_GNDR_ID" === "MALE")).groupBy( $"CRASH_ID").count().select(count($"CRASH_ID"))
result1.coalesce(1).write.mode("overwrite").option("header",true).csv(outputData+"/"+outputFileName+".csv")
}


def Analysis2(inputFileName : String,outputFileName : String): Unit = {
var df_analysis2 = spark.read.option("header",true).csv(inputData+"/"+inputFileName+".csv")
var df_analysis = df_analysis2.distinct()
val result2=df_analysis.filter(($"VEH_BODY_STYL_ID" === "MOTORCYCLE") or ($"VEH_BODY_STYL_ID" === "POLICE MOTORCYCLE") and ($"VEH_PARKED_FL" === "N")).groupBy( $"CRASH_ID").count().select(count($"CRASH_ID"))
result2.coalesce(1).write.mode("overwrite").option("header",true).csv(outputData+"/"+outputFileName+".csv")
}

def Analysis3(inputFileName : String,outputFileName : String): Unit = {
var df_analysis3 = spark.read.option("header",true).csv(inputData+"/"+inputFileName+".csv")
var df_analysis=df_analysis3.distinct()
var result3=df_analysis.filter($"PRSN_GNDR_ID" === "FEMALE").groupBy($"DRVR_LIC_STATE_ID").agg(countDistinct($"CRASH_ID").alias("no_of_accident")).orderBy($"no_of_accident".desc).first()(0)
result3
/*result3.coalesce(1).write.mode("overwrite").option("header",true).csv(outputData+"/"+outputFileName+".csv")*/

}

def Analysis4(inputFileName : String,outputFileName : String): Unit = {
var df_analysis4 = spark.read.option("header",true).csv(inputData+"/"+inputFileName+".csv")
var df_analysis = df_analysis4.distinct()
val result=df_analysis.withColumn("TOT_INJRY",$"TOT_INJRY_CNT" + $"DEATH_CNT").groupBy($"VEH_MAKE_ID").agg(sum($"TOT_INJRY"))
val windowSpec  = Window.partitionBy().orderBy($"sum(TOT_INJRY)".desc)
val result4 = result.withColumn("row_number",row_number().over(windowSpec)).select($"VEH_MAKE_ID").filter(($"row_number">=5) and ($"row_number"<=15))
result4.coalesce(1).write.mode("overwrite").option("header",true).csv(outputData+"/"+outputFileName+".csv")
}

def Analysis5(inputFileName1 : String,inputFileName2 : String,outputFileName : String): Unit = {
var df_analysis_1 = spark.read.option("header",true)csv(inputData+"/"+inputFileName1+".csv")
var df_analysis1 = df_analysis_1.distinct()

var df_analysis_2 = spark.read.option("header",true).csv(inputData+"/"+inputFileName2+".csv")
var df_analysis2 = df_analysis_2.distinct()

var df_Primary_Person_use1 = df_analysis1.withColumn("new_id",concat($"CRASH_ID",$"UNIT_NBR")).select("new_id","PRSN_ETHNICITY_ID")
var df_Units_use1 = df_analysis2.withColumn("new_id",concat($"CRASH_ID",$"UNIT_NBR")).select($"new_id",$"VEH_BODY_STYL_ID")

val df_join = df_Primary_Person_use1.join(df_Units_use1,df_Primary_Person_use1("new_id") === df_Units_use1("new_id"),"inner")
val df = df_join.groupBy("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID").count()

val windowSpec  = Window.partitionBy("VEH_BODY_STYL_ID").orderBy($"count".desc)
val result5 = df.withColumn("row_number",row_number().over(windowSpec)).filter($"row_number" === 1).select($"VEH_BODY_STYL_ID",$"PRSN_ETHNICITY_ID")
result5.coalesce(1).write.mode("overwrite").option("header",true).csv(outputData+"/"+outputFileName+".csv")
}

def Analysis6(inputFileName1 : String,inputFileName2 : String,outputFileName : String): Unit = {
var df_analysis1 = spark.read.option("header",true).csv(inputData+"/"+inputFileName1+".csv")
var df_Primary_Person_use = df_analysis1.distinct()

var df_analysis2 = spark.read.option("header",true).csv(inputData+"/"+inputFileName2+".csv")
var df_Units_use = df_analysis2.distinct()

var df1 = df_Units_use.filter($"VEH_BODY_STYL_ID".isin("POLICE CAR/TRUCK", "PASSENGER CAR, 2-DOOR","PASSENGER CAR, 4-DOOR","SPORT UTILITY VEHICLE")).withColumn("new_id",concat($"CRASH_ID",$"UNIT_NBR")).select("new_id","VEH_BODY_STYL_ID")

var df2 = df_Primary_Person_use.filter($"PRSN_ALC_RSLT_ID" === "Positive").withColumn("new_id",concat($"CRASH_ID",$"UNIT_NBR")).select("new_id","DRVR_ZIP")
var df3=df1.join(df2,df1("new_id") === df2("new_id"),"inner")

var df_res6 = df3.filter($"DRVR_ZIP" !== "null").groupBy($"DRVR_ZIP").count()

var windowSpec  = Window.partitionBy().orderBy($"count".desc)
var result6 = df_res6.withColumn("row_number",row_number().over(windowSpec)).filter(($"row_number">=1) and ($"row_number"<=5)).select($"DRVR_ZIP")
result6.coalesce(1).write.mode("overwrite").option("header",true).csv(outputData+"/"+outputFileName+".csv")
}


def Analysis7(inputFileName1 : String,inputFileName2 : String,outputFileName : String): Unit = {
var df_analysis1 = spark.read.option("header",true).csv(inputData+"/"+inputFileName1+".csv")
var df_Damages_use = df_analysis1.distinct()

var df_analysis2 = spark.read.option("header",true).csv(inputData+"/"+inputFileName2+".csv")
var df_Units_use = df_analysis2.distinct()

var df_Damages_use_crash_id=df_Damages_use.select($"CRASH_ID").distinct()

var df_res7=df_Units_use.filter($"FIN_RESP_TYPE_ID".isin("INSURANCE BINDER","LIABILITY INSURANCE POLICY","CERTIFICATE OF SELF-INSURANCE") and $"VEH_BODY_STYL_ID".isin("POLICE CAR/TRUCK", "PASSENGER CAR, 2-DOOR","PASSENGER CAR, 4-DOOR","SPORT UTILITY VEHICLE") and $"VEH_DMAG_SCL_1_ID".isin("DAMAGED 5","DAMAGED 6","DAMAGED 7 HIGHEST") or $"VEH_DMAG_SCL_2_ID".isin("DAMAGED 5","DAMAGED 6","DAMAGED 7 HIGHEST")).select($"CRASH_ID")

var result7=df_res7.join(df_Damages_use_crash_id,df_res7("CRASH_ID") === df_Damages_use_crash_id("CRASH_ID"),"leftanti").count()

result7
}


Analysis1("Primary_Person_use","Analysis1_result")
Analysis2("Units_use","Analysis2_result")
Analysis3("Primary_Person_use","Analysis3_result")
Analysis4("Units_use","Analysis4_result")
Analysis5("Primary_Person_use","Units_use","Analysis5_result")
Analysis6("Primary_Person_use","Units_use","Analysis6_result")
Analysis7("Damages_use","Units_use","Analysis6_result")   

println("File sent to sink location")

}