/**
 * @author Chandresh Bhatt
 * 
 * 		--> KPI4 : Filter all the records with event_id = 118
 * 	  --> Get the maximum and minimum duration
 * ------------------------------------------------------------------------------------------- 
 * Output :
 * Max. Duration : 21600 =>,  Min. Duration : 577
 * 
 * 
 * 
 */

package com.practice.dataflair.stb

import org.apache.spark.sql.SparkSession

object SetTopBox_KPI4 {
 
  def main(args: Array[String]): Unit = {
    if(args.length < 2) {
      System.err.println("SetTopBox Data Analysis <Input-File> OR <Output-File> is missing")
      System.exit(1)
    }
    
    val spark = SparkSession.builder().appName("KPI4").getOrCreate()
    
    val record = spark.read.textFile(args(0)).rdd
    
    val eventid_118 = record.filter { x => x.contains("^118^") }
                            .map { x => x.replace("^", "#") }
    
    val removetextdata = eventid_118.flatMap { x => x.split("#").drop(4).dropRight(2) }
    
    val formatstring = removetextdata.map { x => x.toString().trim() }
    
    val xmltags = formatstring.map { x => xml.XML.loadString(x) }
    
    val xmltags1 = xmltags.map { x => x.\\("nv") }
   
    val durationinsectag = xmltags1.map { x => x.theSeq(x.length-1) }
    
    val durationinsecsvalues = durationinsectag.map { x => (x \\ "nv" \ "@v").toString().toInt }
    
    val maxduration = durationinsecsvalues.max()
    
    val minduration = durationinsecsvalues.min()
        
    println("Max. Duration : " + maxduration + " => Min. Duration : " + minduration)
        
    //durationinsecsvalues.saveAsTextFile(args(1))
    spark.stop()
  }
}
//bin/spark-submit --class com.practice.dataflair.stb.SetTopBox_KPI4 ../stb4.jar ../Set_Top_Box_Data.txt stb4-out-001
//bin/spark-submit --class com.practice.dataflair.stb.SetTopBox_KPI7_2 ../stb7_2.jar ../Set_Top_Box_Data.txt stb7_2-out-001