/* Importing all the required libraries */

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object BcgCaseStudy extends App {

/* inputData : command line argument (path of input csv files stored in HDFS file location)*/
/* outputData : command line argument (path of output files stored in Unix Box file location)*/

val inputData=args(0)
val outputData=args(1)

/* Creating SparkSession in spark variable*/

val spark = SparkSession.builder().master("yarn").appName("BcgCaseStudy").getOrCreate()
import spark.implicits._

/* Reading input csv files in dataFrame 
   inputData : command line argument
*/

var df_Primary_Person_use = spark.read.option("header",true).csv(inputData+"/Primary_Person_use.csv").distinct()
var df_Units_use = spark.read.option("header",true).csv(inputData+"/Units_use.csv").distinct()
var df_Damages_use = spark.read.option("header",true).csv(inputData+"/Damages_use.csv").distinct()
var df_Endorse_use = spark.read.option("header",true).csv(inputData+"/Endorse_use.csv").distinct()
var df_Charges_use = spark.read.option("header",true).csv(inputData+"/Charges_use.csv").distinct()
var df_Restrict_use = spark.read.option("header",true).csv(inputData+"/Restrict_use.csv").distinct()

/* Below functions are displaying and saving the final dataframe at the provided location in the command line argument with output file names provided in function parameter*/

/* 1.Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male? */

def Analysis1(outputFileName : String): Unit = {
val result1=df_Primary_Person_use.filter(( $"DEATH_CNT">0) and ( $"PRSN_GNDR_ID" === "MALE")).groupBy( $"CRASH_ID").count().select(count($"CRASH_ID"))
display(result1)
result1.coalesce(1).write.mode("overwrite").option("header",true).csv(outputData+"/"+outputFileName)
}

/* 2.Analysis 2: How many two wheelers are booked for crashes? */

def Analysis2(outputFileName : String): Unit = {
val result2=df_Units_use.filter(($"VEH_BODY_STYL_ID" === "MOTORCYCLE") or ($"VEH_BODY_STYL_ID" === "POLICE MOTORCYCLE") and ($"VEH_PARKED_FL" === "N")).groupBy( $"CRASH_ID").count().select(count($"CRASH_ID"))
display(result2)  
result2.coalesce(1).write.mode("overwrite").option("header",true).csv(outputData+"/"+outputFileName)
}

/* 3.Analysis 3: Which state has highest number of accidents in which females are involved?  */

def Analysis3(outputFileName : String): Unit = {
val result=df_Primary_Person_use.filter($"PRSN_GNDR_ID" === "FEMALE").groupBy($"DRVR_LIC_STATE_ID").agg(countDistinct($"CRASH_ID").alias("no_of_accident"))
val windowSpec  = Window.partitionBy().orderBy($"no_of_accident".desc)
val result3 = result.withColumn("row_number",row_number().over(windowSpec)).select($"DRVR_LIC_STATE_ID").filter($"row_number" === 1)
display(result3)
result3.coalesce(1).write.mode("overwrite").option("header",true).csv(outputData+"/"+outputFileName)
}

/* 4.Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death */

def Analysis4(outputFileName : String): Unit = {
val result=df_Units_use.withColumn("TOT_INJRY",$"TOT_INJRY_CNT" + $"DEATH_CNT").groupBy($"VEH_MAKE_ID").agg(sum($"TOT_INJRY"))
val windowSpec  = Window.partitionBy().orderBy($"sum(TOT_INJRY)".desc)
val result4 = result.withColumn("row_number",row_number().over(windowSpec)).select($"VEH_MAKE_ID").filter(($"row_number">=5) and ($"row_number"<=15))
display(result4)
result4.coalesce(1).write.mode("overwrite").option("header",true).csv(outputData+"/"+outputFileName)
}

/* Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style */

def Analysis5(outputFileName : String): Unit = {

var df_Primary_Person_use1 = df_Primary_Person_use.withColumn("new_id",concat($"CRASH_ID",$"UNIT_NBR")).select("new_id","PRSN_ETHNICITY_ID")
var df_Units_use1 = df_Units_use.withColumn("new_id",concat($"CRASH_ID",$"UNIT_NBR")).select($"new_id",$"VEH_BODY_STYL_ID")

val df_join = df_Primary_Person_use1.join(df_Units_use1,df_Primary_Person_use1("new_id") === df_Units_use1("new_id"),"inner")
val df_grp = df_join.groupBy("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID").count()

val windowSpec  = Window.partitionBy("VEH_BODY_STYL_ID").orderBy($"count".desc)
val result5 = df_grp.withColumn("row_number",row_number().over(windowSpec)).filter($"row_number" === 1).select($"VEH_BODY_STYL_ID",$"PRSN_ETHNICITY_ID")
display(result5)
result5.coalesce(1).write.mode("overwrite").option("header",true).csv(outputData+"/"+outputFileName)
}

/* Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code) */

def Analysis6(outputFileName : String): Unit = {

var df1 = df_Units_use.filter($"VEH_BODY_STYL_ID".isin("POLICE CAR/TRUCK", "PASSENGER CAR, 2-DOOR","PASSENGER CAR, 4-DOOR","SPORT UTILITY VEHICLE")).withColumn("new_id",concat($"CRASH_ID",$"UNIT_NBR")).select("new_id","VEH_BODY_STYL_ID")

var df2 = df_Primary_Person_use.filter($"PRSN_ALC_RSLT_ID" === "Positive").withColumn("new_id",concat($"CRASH_ID",$"UNIT_NBR")).select("new_id","DRVR_ZIP")
var df3=df1.join(df2,df1("new_id") === df2("new_id"),"inner")

var df_res6 = df3.filter($"DRVR_ZIP" !== "null").groupBy($"DRVR_ZIP").count()

var windowSpec  = Window.partitionBy().orderBy($"count".desc)
var result6 = df_res6.withColumn("row_number",row_number().over(windowSpec)).filter(($"row_number">=1) and ($"row_number"<=5)).select($"DRVR_ZIP")
  
display(result6)
result6.coalesce(1).write.mode("overwrite").option("header",true).csv(outputData+"/"+outputFileName)
}

/* Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance */

def Analysis7(outputFileName : String): Unit = {

var df_Damages_use_crash_id=df_Damages_use.select($"CRASH_ID").distinct()
var df_res7=df_Units_use.filter($"FIN_RESP_TYPE_ID".isin("INSURANCE BINDER","LIABILITY INSURANCE POLICY","CERTIFICATE OF SELF-INSURANCE") and $"VEH_BODY_STYL_ID".isin("POLICE CAR/TRUCK", "PASSENGER CAR, 2-DOOR","PASSENGER CAR, 4-DOOR","SPORT UTILITY VEHICLE") and $"VEH_DMAG_SCL_1_ID".isin("DAMAGED 5","DAMAGED 6","DAMAGED 7 HIGHEST") or $"VEH_DMAG_SCL_2_ID".isin("DAMAGED 5","DAMAGED 6","DAMAGED 7 HIGHEST")).select($"CRASH_ID")

var result7=df_res7.join(df_Damages_use_crash_id,df_res7("CRASH_ID") === df_Damages_use_crash_id("CRASH_ID"),"leftanti").count()

/* Analysis7 function only print the ouutput at saprk-shell */

result7
}

/* Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data) */

def Analysis8(outputFileName : String): Unit = {

val df_license=df_Primary_Person_use.filter(($"DRVR_LIC_TYPE_ID" === "DRIVER LICENSE") or ($"DRVR_LIC_TYPE_ID" === "COMMERCIAL DRIVER LIC.") or ($"DRVR_LIC_TYPE_ID" === "OCCUPATIONAL")).filter($"DRVR_LIC_CLS_ID" =!= "UNLICENSED").select("crash_id","DRVR_LIC_STATE_ID").distinct()
 
val list_of_top_10_colors  = df_Units_use.filter($"VEH_COLOR_ID" =!= "NA").groupBy("VEH_COLOR_ID").count().orderBy($"count".desc)

var windowSpec  = Window.partitionBy().orderBy($"count".desc)
var list_of_top_10_colors_df = list_of_top_10_colors.withColumn("row_number",row_number().over(windowSpec)).filter(($"row_number">=1) and ($"row_number"<=10)).select($"VEH_COLOR_ID")

val df_Units_use_clr_join=df_Units_use.join(list_of_top_10_colors_df,df_Units_use("VEH_COLOR_ID") === list_of_top_10_colors_df("VEH_COLOR_ID"),"inner")

val df_unit_clr_car=df_Units_use_clr_join.filter($"VEH_BODY_STYL_ID".isin("POLICE CAR/TRUCK", "PASSENGER CAR, 2-DOOR","PASSENGER CAR, 4-DOOR","SPORT UTILITY VEHICLE")).select("CRASH_ID","VEH_MAKE_ID").distinct()


val df_speed_charges=df_Charges_use.filter(lower($"CHARGE").contains("speed")).select("crash_id").distinct()

val df8_join=df_speed_charges.join(df_unit_clr_car,Seq("crash_id"),"inner").join(df_license,Seq("crash_id"),"inner")

  
val list_of_top_25_states = df8_join.filter($"DRVR_LIC_STATE_ID" =!= "Other").groupBy("DRVR_LIC_STATE_ID").count().orderBy($"count".desc)

val list_of_top_25_states_df = list_of_top_25_states.withColumn("row_number",row_number().over(windowSpec)).filter(($"row_number">=1) and ($"row_number"<=25))
.select($"DRVR_LIC_STATE_ID")

val df9=df8_join.join(list_of_top_25_states_df,Seq("DRVR_LIC_STATE_ID"),"inner")

val list_of_top_5_veh_maker=df9.groupBy("VEH_MAKE_ID").count().orderBy($"count".desc)

val result8=list_of_top_5_veh_maker.withColumn("row_number",row_number().over(windowSpec)).filter(($"row_number">=1) and ($"row_number"<=5)).select("VEH_MAKE_ID")

display(result8)
result8.coalesce(1).write.mode("overwrite").option("header",true).csv(outputData+"/"+outputFileName+".csv")

}

/* fUNCTION CALLS WITH OUTPUT FILE NAME AS A PARAMETER */

Analysis1("Analysis1_output")  
Analysis2("Analysis2_output")
Analysis3("Analysis3_output")
Analysis4("Analysis4_output")
Analysis5("Analysis5_output")
Analysis6("Analysis6_output")
Analysis7("Analysis7_output")
Analysis8("Analysis8_output")

}