/**
 * @author Chandresh Bhatt
 * 
 * KPI7_2 :
 * 		--> Filter all the record with Event ID 115 / 118
 *    --> Total No. of Devices with Frequency = "Once"
 * 
 * Output :
 * Total No. of Devices with Frequency = 'Once' => 922
 *  
 */

package com.practice.dataflair.stb

import org.apache.spark.sql.SparkSession



object SetTopBox_KPI7_2 {
  def main(args: Array[String]): Unit = {
    
    if(args.length <2) {
      System.err.println("SetTopBox Analysis : <Input-file> OR <Output-file> is missing")
      System.exit(1)
    }
    
    val spark = SparkSession.builder().appName("").getOrCreate()
    val record = spark.read.textFile(args(0)).rdd
    val eventid_100 = record.filter {
                                      filterlines_100 => filterlines_100.contains("^118^") 
                                    }
                                    .map {
                                      x => x.replace("^", "#")
                                    }
                                    
    val onlyxmltags = eventid_100.flatMap { x => x.split("#").drop(4).dropRight(2) }
    
    val xmlstring = onlyxmltags.map { x => x.toString().trim() }
    
    val loadxml = xmlstring.map { x => xml.XML.loadString(x) }
    
    val attributewithnv = loadxml.map { x => x.\\("nv") }                                      
    
    val freqonly = attributewithnv.map { x => x.theSeq(7).attribute("v").get }
    
    val freqonly2 = freqonly.filter { x => x.text.equals("Once") }

    val freqcount = freqonly2.count()
    
    println("Total No. of Devices with Frequency = 'Once' => " +  freqcount)
    
    freqonly2.saveAsTextFile(args(1))
    spark.stop()
  }     
}

//bin/spark-submit --class com.practice.dataflair.stb.SetTopBox_KPI7_2 ../stb7_2.jar ../Set_Top_Box_Data.txt stb7_2-out-001
