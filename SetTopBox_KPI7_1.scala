/**
 *  @author Chandresh Bhatt
 *  
 *  KPI7_1:
 *  			-->	Filter all the records with EventID 115 / 118
 *  					--> Get the duration group by Program_ID
 *  
 *  Output : [Excerpt from Output File]
 *  
 *  (1337fe22-0000-0000-0000-000000000000,CompactBuffer(7265, 7260, 7260, 7260, 7259, 7265, 7260, 7260, 7260, 7259, 7265, 7260, 7260, 7260, 7259, 7265, 7260, 7260, 7260, 7259, 7265, 7260, 7260, 7260, 7259, 7200, 7200, 7200, 7200))
 *  (0ed5235b-0000-0000-0000-000000000000,CompactBuffer(3600))
*   (110b6992-0000-0000-0000-000000000000,CompactBuffer(5400))
*   (13bebe1e-0000-0000-0000-000000000000,CompactBuffer(1870, 1870, 1870, 1870, 1870, 1800))
*   (003730ee-0000-0000-0000-000000000000,CompactBuffer(7200))
*   (144005d7-0000-0000-0000-000000000000,CompactBuffer(3671, 3660, 3671, 3660, 3671, 3660, 3671, 3660, 3671, 3660))
*   (13bebe05-0000-0000-0000-000000000000,CompactBuffer(1870, 1870, 1870, 1870, 1870, 1800, 1800, 1800))
*   (0036d608-0000-0000-0000-000000000000,CompactBuffer(7260, 7260, 7260, 7260, 7260))
*   (1410d7a1-0000-0000-0000-000000000000,CompactBuffer(3660, 3669, 3660, 3670, 3660, 3669, 3660, 3670, 3660, 3669, 3660, 3670, 3660, 3669, 3660, 3670, 3660, 3669, 3660, 3670))
 *  
 */

package com.practice.dataflair.stb

import org.apache.spark.sql.SparkSession

object SetTopBox_KPI7_1 {
   def main(args: Array[String]): Unit = {
     if(args.length < 2) {
     System.err.println("SetTopBox Data Analysis <Input-File> OR <Output-File> is missing")
     System.exit(1)
   }
   
   val spark = SparkSession.builder().appName("KPI7_1").getOrCreate()
   
   val record = spark.read.textFile(args(0)).rdd
   
   val event_id_115_118 = record.filter { x => (x.contains("^115^") || x.contains("^118^")) }
                                                          .map { x => x.replace("^", "#") } 
   
   val xmltags1 = event_id_115_118.map { x => x.substring(x.indexOf("<"), x.lastIndexOf("#")) }
   
   val xmltags2 = xmltags1.map { x => x.substring(x.indexOf("<"), x.lastIndexOf("#")) }
   
   val strtoxml = xmltags2.map { x => xml.XML.loadString(x) }
   
   val xmlattributes = strtoxml.map { x => x.\\("nv") }
   
   val tuple2_progamid_duration = xmlattributes.map { x => (x.theSeq(1), x.theSeq(x.length-1))}
   
   val tuple2_programid_duration_value = tuple2_progamid_duration.map(x => (x._1 \\ "nv" \ "@v", x._2 \\ "nv" \ "@v"))
   
   val durationgroupbyprogramid = tuple2_programid_duration_value.groupByKey()
      
   durationgroupbyprogramid.saveAsTextFile(args(1))
   
   spark.stop()
                                                          
                                                          
   //val xmltags = event_id_115_118.flatMap { x => x.split("#").drop(4).dropRight(2) }
   
   //val removespaceifany = xmltags.map { x => x.toString().trim() }
   
   //------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------//
   //removeheadtag and removetailtag are to handle the SAXParseException line 1, column1 --> content not allowed in prolog
   //val removeheadtag = removespaceifany.map { x => x.replaceAll("<d>", "") }
   
   //val removetailtag = removeheadtag.map { x => x.replaceAll("</d>", "") }
   //------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------//
                                                              
   //val strtoxml = removetailtag.map { x => xml.XML.loadString("<r>" + x + "</r>") }
   
   //val strtoxml = removespaceifany.map { x => xml.XML.loadString(x) }
   
   //val xmlattributes = strtoxml.map { x => x.\\("nv") }
   
   //val tuple2_progamid_duration = xmlattributes.map { x => x.theSeq(x.length)}
      
   
   }    
}

//bin/spark-submit --class  com.practice.dataflair.stb.SetTopBox_KPI7_1 ../stb7_1.jar ../Set_Top_Box_Data.txt stb7_1-out-001
//bin/spark-submit --class com.practice.dataflair.stb.SetTopBox_KPI1_1 ../stb1_1.jar ../Set_Top_Box_Data.txt stb1_1-out-001