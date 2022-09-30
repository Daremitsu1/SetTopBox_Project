/**
 * 
 * KPI1_2:
 * 				--> Filter all the records with event id = 100
 * 				--> Get the top five channels with maximum duration
 * 
 * Output :
 * Duration = 28929343, ChannelNumbner = 138
 * Duration = 28788598, ChannelNumbner = 1606
 * Duration = 28788140, ChannelNumbner = 138
 * Duration = 28783711, ChannelNumbner = 1604
 * Duration = 28774440, ChannelNumbner = 326
 * 
 */
package com.practice.dataflair.stb

import org.apache.spark.sql.SparkSession

object SetTopBox_KPI1_2 {
  def main(args: Array[String]): Unit = {
    if(args.length < 2){
      System.err.println("Set Top Box Analysis : <Input-file> OR <Output-file> is missing")
      System.exit(1)
    }
    val spark = SparkSession.builder().appName("KPI1_2").getOrCreate()
    val record = spark.read.textFile(args(0)).rdd
    
    val eventid_100 = record.filter { x => x.contains("^100^")}
                            .map { x => x.replace("^", "#") }
    
    val xmltags = eventid_100.flatMap { x => x.split("#").drop(4).dropRight(2) }
    
    val xmltagstrim = xmltags.map { x => x.toString().trim() }
    
    val strtoxml = xmltagstrim.map { x => xml.XML.loadString(x) }
    
    val attributes = strtoxml.map { x => x.\\("nv")}
    
    val channelnumber_duration = attributes.map { x => x.theSeq(2) + x.theSeq(3).toString() }
    
    val filterchannelnumber = channelnumber_duration.filter { x => x.contains("ChannelNumber") }
    
    val toxmlagain = filterchannelnumber.map { x => xml.XML.loadString("<d>" + x + "</d>") }
    
    val finalattributes = toxmlagain.map { x => x.\\("nv") }
            
    val duration = finalattributes.map { x =>  (x.theSeq(1).attribute("v").get.text.toInt, x.theSeq(0).attribute("v").get.text.toInt) }.sortByKey(false) 
    
    
    duration.take(5).foreach(x => x.productIterator.foreach { println})
    
    
    duration.saveAsTextFile(args(1))                     
    spark.stop()
      
  }
}

//bin/spark-submit --class com.practice.dataflair.stb.SetTopBox_KPI1_2 ../stb1_2.jar ../Set_Top_Box_Data.txt stb1_2-out-001
//bin/spark-submit --class com.practice.dataflair.stb.SetTopBox_KPI1_1 ../stb1_1.jar ../Set_Top_Box_Data.txt stb1_1-out-001