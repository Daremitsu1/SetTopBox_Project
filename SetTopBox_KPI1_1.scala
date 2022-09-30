/**
 * 
 * KPI1_1 :
 * 					--> Filter all the records with Event_ID = 100
 * 						--> Get the top five devices with maximum duration
 * 
 * Output : Excerpt from Output File
 * (28929343,01fb1ead-5422-4cab-a3b7-40cba65e1b50)
 * (28788598,02557de9-1fdf-48fb-b419-3e9e31ac3ccf)
 * (28788140,01fb1ead-5422-4cab-a3b7-40cba65e1b50)
 * (28783711,02557de9-1fdf-48fb-b419-3e9e31ac3ccf)
 * (28774440,026eda68-0f8d-4e13-80ba-72bed26ce576)
 * (28768034,01fb1ead-5422-4cab-a3b7-40cba65e1b50)
 * (28763687,026eda68-0f8d-4e13-80ba-72bed26ce576)
 * (28755174,02b78174-433f-4306-959d-8e5f4e3060d4)
 * (28750201,01268703-4d56-4976-97ec-582a0f1af6db)
 * (28744574,01268703-4d56-4976-97ec-582a0f1af6db)
 * (26691317,03620c62-6477-42ca-8685-a3e3646dcdd6)
 * (26148879,01beb30a-db89-4c3c-9340-dee9e98bc00c)
 * (24643168,024504b2-241b-4732-9643-4e6ba7d51887)
 * (23953340,026685d5-fd71-4fd5-a76b-9e0a862d027b)
 * (22334614,0144801e-ca63-42c5-89bb-8b980a6e4532)
 * (17527966,02b78174-433f-4306-959d-8e5f4e3060d4)
 * (16631268,02b78174-433f-4306-959d-8e5f4e3060d4)
 * (15568264,03426277-db6c-47d0-a521-14738d3cc874)
 * (15201086,01beb30a-db89-4c3c-9340-dee9e98bc00c)
 * (14397541,01e62106-0b0b-42dd-a583-fea709c9df04)
 * (14001541,0144801e-ca63-42c5-89bb-8b980a6e4532)
 * 
 * 
 * 
 * 
 */
package com.practice.dataflair.stb

import org.apache.spark.sql.SparkSession

object SetTopBox_KPI1_1 {
    
  def main(args: Array[String]): Unit = {
    if(args.length < 2) {
      System.err.println("SetTopBox Data Analysis <Input-File> OR <Output-File> is missing")
      System.exit(1)
    }
    
    val spark = SparkSession.builder().appName("KPI1_1").getOrCreate()
    
    val record = spark.read.textFile(args(0)).rdd
    
    val eventid_100 = record.filter { x => x.contains("^100^") }
                            .map { x => x.replace("^", "#") }
                            
    val xml_str = eventid_100.map { x => x.substring(x.indexOf("<"), x.lastIndexOf("#")) }                        
    
    val tuple2_xml_str = xml_str.map { x => (x.substring(x.indexOf("<"), x.indexOf("#")), x.substring(x.indexOf("#")+1, x.length())) }
                            
    val tuple2_toxml_str = tuple2_xml_str.map(x => (xml.XML.loadString(x._1),x._2))
                                        
    val tuple2_processing_1 = tuple2_toxml_str.map(x => (x._1.\\("nv"), x._2))
    
    val tuple2_duration_device = tuple2_processing_1.map(x => ((x._1.theSeq(x._1.length-5)), x._2))
    
    val tuple2_duration_device2 = tuple2_duration_device.map(x => (x._1 \\"nv" \ "@v", x._2))
    
    val tuple2_durationtoInt_device = tuple2_duration_device2.map(x => (x._1.text.toInt, x._2))
    
    val tuple2_maxduration_withdevice = tuple2_durationtoInt_device.sortByKey(false)
                            
    tuple2_maxduration_withdevice.saveAsTextFile(args(1))                       
//------------------------------------------------------------------------------------------------------------------------------------//                                                          
//    val deviceid =  eventid_100.flatMap { x => (x.split("#").drop(5).dropRight(1))}.map { x => x.toString().trim() }
//    
//    val xmltagsonly = eventid_100.flatMap { x => x.split("#").drop(4).dropRight(2) }.map { x => x.toString().trim() }
//    
//    val toxml = xmltagsonly.map { x => xml.XML.loadString(x)}     
//            
//    val adddeviceid = toxml.map { x => x + ("<nv v=" + deviceid.toString().toCharArray() + " " + "n=" + "\"DeviceID\"" + "/>") }
//                               
//    adddeviceid.saveAsTextFile(args(1))
//------------------------------------------------------------------------------------------------------------------------------------//    
    spark.stop()
  }
}

//bin/spark-submit --class com.practice.dataflair.stb.SetTopBox_KPI1_1 ../stb1_1.jar ../Set_Top_Box_Data.txt stb1_1-out-001