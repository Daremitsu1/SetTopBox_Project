/**
 * @author Chandresh Bhatt
 * 
 * KPI5 :
 * 			--> Filter all the record with EventID 0
 * 					--> Calculate how many junk records are their having BadBlocks in XML Column
 * 
 * Output :
 * 								BadBlocks in XML : 517
 * (BadBlocks,257)
* (BadBlocks,260)
* (BadBlocks,256)
* (BadBlocks,257)
* (BadBlocks,257)
* (BadBlocks,256)
* (BadBlocks,257)
* (BadBlocks,256)
* (BadBlocks,257)
* (BadBlocks,1)
* (BadBlocks,256)
* (BadBlocks,256)
* (BadBlocks,258)
* (BadBlocks,256)
* (BadBlocks,256)
* (BadBlocks,1)
* (BadBlocks,256)
* (BadBlocks,257)
* (BadBlocks,257)
* (BadBlocks,257)
* (BadBlocks,256)
* (BadBlocks,256)
* (BadBlocks,256)
* (BadBlocks,256)
* (BadBlocks,258)
* (BadBlocks,257)
* (BadBlocks,257)
* (BadBlocks,256)
* (BadBlocks,257)
* (BadBlocks,257)
* (BadBlocks,257)
* (BadBlocks,257)
* (BadBlocks,257)
* 
* 
* 
*/


package com.practice.dataflair.stb

import org.apache.spark.sql.SparkSession

object SetTopBox_KPI5 {
 
  def main(args: Array[String]): Unit = {
    if(args.length < 2) {
      System.err.println("SetTopBox Data Analysis <Input-File> OR <Output-File> is missing")
      System.exit(1)
    }
    
    val spark = SparkSession.builder().appName("KPI5").getOrCreate()
    
    val record = spark.read.textFile(args(0)).rdd
    
    val eventid_0 = record.filter { x => x.contains("^0^") }
                                           .map { x => x.replace("^", "#") }
    
    val xmltags1 = eventid_0.map { x => x.substring(x.indexOf("<"), x.lastIndexOf("#")) }
    
    val xmltags2 = xmltags1.map { x => x.substring(x.indexOf("<"), x.lastIndexOf("#")) }
        
    //----------------------------------------------------------------------------------------------------------------//
    //val xmltags1 = eventid_0.map { x => x.substring(x.indexOf("<"), x.lastIndexOf("#")) }
   
    //val xmltags2 = xmltags1.map { x => x.substring(x.indexOf("<"), x.lastIndexOf("#")) }
    //----------------------------------------------------------------------------------------------------------------//
                                           
    //----------------------------------------------------------------------------------------------------------------//                                       
    //val xmltags = eventid_0.flatMap { x => x.split("#").drop(4).dropRight(2)}
    
    //val removespace = xmltags.map { x => x.toString().trim() }
    //----------------------------------------------------------------------------------------------------------------//
    
    //val strtoxml = xmltags2.map { x => xml.XML.loadString(x) }
                      
    //val xmlattributes = strtoxml.map { x => x.\\("nv") }
    
    //val getbadblks = xmlattributes.map { x => x.theSeq(1).toString() }
    
    val loadxml = xmltags2.map { x => xml.XML.loadString(x) }
    
    val xmlattributes = loadxml.map { x => x.\\("nv") }
    
    val getbadblks = xmlattributes.map { x => x.take(2).takeRight(1)}
    
    val filterbadblks1 = getbadblks.map { x => (x \\ "nv" \ "@n", x \\ "nv" \ "@v") }
    
    val filterbadblks2 = filterbadblks1.filter(x => (x._1.text.toString().equals("BadBlocks")))
                                                        
     val badblkwhichisnotzero = filterbadblks2.filter(x => (x._2.text.toInt != 0))
     
     println("BadBlocks in XML : " + badblkwhichisnotzero.count())
    
    badblkwhichisnotzero.saveAsTextFile(args(1))
    
    spark.stop()
  }
}
//bin/spark-submit --class com.practice.dataflair.stb.SetTopBox_KPI5 ../stb5.jar ../Set_Top_Box_Data.txt stb5-out-001
//bin/spark-submit --class com.practice.dataflair.stb.SetTopBox_KPI4 ../stb4.jar ../Set_Top_Box_Data.txt stb4-out-001
