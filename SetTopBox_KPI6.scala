/**
 * @author Chandresh Bhatt
 * 
 * KPI 6 :
 * 	--> Filter all the records with Event 107
 * 			--> Group all the ButtonNames with their Device IDs
 * 
 * Output : (Excerpt from OutputFile)
 * (33a6885a-b8d1-4695-98df-8ef314437c7e,CompactBuffer(Scheduled, Scheduled, Scheduled, Scheduled, Scheduled, Scheduled, Scheduled, Scheduled, Scheduled, Scheduled, Scheduled, Scheduled))
*  (1e50202b-1366-4be2-a4c4-6a66c89c2226,CompactBuffer(Guide, Recorded, Guide, Recorded, Guide, Recorded, Guide, Recorded, Guide, Recorded, Guide, Recorded))
*  (469c6308-c756-413d-b03a-219edbf766b4,CompactBuffer(Recorded, Recorded, Recorded, Recorded, Recorded, Recorded))
*  (3e94908d-57f3-45d8-89bd-b6ff8d5bbeaa,CompactBuffer(Favorites, Favorites, Recorded, Favorites, Favorites, Recorded, Favorites, Favorites, Recorded, Favorites, Favorites, Recorded, Favorites, Favorites, Recorded, Favorites, Favorites, Recorded))
*  (0fba8490-58e9-4b7f-b3ff-7b823e8f8acc,CompactBuffer(Favorites, Favorites, Favorites, Favorites, Favorites, Favorites))
*  (0848c5f5-6f48-4d04-831b-49b3a9a74daf,CompactBuffer(Recorded, Recorded, Scheduled, Scheduled, Recorded, Recorded, Recorded, Scheduled, Scheduled, Recorded, Recorded, Recorded, Scheduled, Scheduled, Recorded, Recorded, Recorded, Scheduled, Scheduled, Recorded, Recorded, Recorded, Scheduled, Scheduled, Recorded, Recorded, Recorded, Scheduled, Scheduled, Recorded))
*  (395c7782-58ff-453e-b0b7-ab0b94a076d6,CompactBuffer(Guide, Guide, Guide, Guide, Guide, Guide))
*  (27cd125f-eb64-47d3-bbe7-e674955903ae,CompactBuffer(Guide, Guide, Guide, Guide, Guide, Guide))
*  (04cab9cf-f8e8-4c05-985f-a553429e772b,CompactBuffer(Closed Captioning, Closed Captioning, Closed Captioning, Closed Captioning, Closed Captioning, Closed Captioning))
*  (2f452c6f-4def-45eb-ae61-f3f3517e330d,CompactBuffer(Recorded, Recorded, Recorded, Recorded, Recorded, Recorded))
*  (1cc97f16-edf9-4daa-9360-aa01370cbf3a,CompactBuffer(Search, Search, Search, Search, Search, Search))
*  (08fe3e45-528a-449c-b438-ff12c3b315fe,CompactBuffer(Favorites, Favorites, Favorites, Favorites, Favorites, Favorites))
*  (20943d7a-e330-4736-8a74-102d721927e0,CompactBuffer(Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded, Recorded))
 * 
 *
 */
package com.practice.dataflair.stb

import org.apache.spark.sql.SparkSession

object SetTopBox_KPI6 {
	
  def main(args: Array[String]): Unit = {
	  if(args.length < 2) {
	    System.err.println("SetTopBox Data Analysis <Input-File> OR <Output-File> is missing")
	    System.exit(1)
	  }
	  
	  val spark = SparkSession.builder().appName("KPI6").getOrCreate()
	  
	  val record = spark.read.textFile(args(0)).rdd
	  
	  val eventid_107 = record.filter {x => x.contains("^107^") }
	                          .map { x => x.replace("^", "#") }
	  
	  val xml_deviceid = eventid_107.map { x => x.substring(x.indexOf("<"))}
	  val tuple2_xml_deviceid = xml_deviceid.map { x => x.substring(x.indexOf("<"), x.lastIndexOf("#")) }
	  val tuple2_xmldeviceidonly = tuple2_xml_deviceid.map { x => (x.substring(x.indexOf("<"), x.indexOf("#")), x.substring(x.indexOf("#")+1, x.length())) }
	  val tuple2_toxml_str = tuple2_xmldeviceidonly.map(x => (xml.XML.loadString(x._1), x._2))
	  val tuple2_process_1 = tuple2_toxml_str.map(x => (x._1.\\("nv"), x._2))
	  val tuple2_process_2 = tuple2_process_1.map(x => (x._1.theSeq(0), x._2))
	  val tuple2_process_3 = tuple2_process_2.map(x => (x._1 \\ "nv" \ "@v", x._2)) 
	  val swaptuple2 = tuple2_process_3.map(x => (x._1.text.toString(), x._2).swap)
	  val buttonnamegroupbydeviceid = swaptuple2.groupByKey()
	  
	                          
	  buttonnamegroupbydeviceid.saveAsTextFile(args(1))
//-----------------------------------------------------------------------------------------------------------------------------------
//    Below code is running fine but has problem to make the tuple at the end
//   	i.e. if first element is deviceid then issue is to make the tuple of deviceid , buttonnames and vice versa
//    it says that (deiveid, Mappartitioned at [11])	  
//	  val getdeviceid = eventid_107.flatMap { x => (x.split("#").drop(5).dropRight(1))}
//	   
//	  val getxmltags = eventid_107.flatMap { x => x.split("#").drop(4).dropRight(2) }
//	  	   
//	  val strtoxml = getxmltags.map { x => xml.XML.loadString(x) }
//	  	  
//	  val allattributes = strtoxml.map { x => x.\\("nv") }
//	  
//	  val buttonnameattribute = allattributes.map { x => x.theSeq(0) }
//	  
//	  val buttonnames = buttonnameattribute.map { x => (x \\ "nv" \ "@v")}
//	  
//	  val deviceid = getdeviceid.map { x => x.toString() }
//	  
//	  //Running
//	  //val buttonnamesstring = buttonnames.map { x => x.text.toString()} 
//	  
//    //val deviceid_btnname = buttonnameasstring.groupByKey()
//-----------------------------------------------------------------------------------------------	  

	  spark.stop()
	}
}

//bin/spark-submit --class com.practice.dataflair.stb.SetTopBox_KPI6 ../stb6.jar ../Set_Top_Box_Data.txt stb6-out-001




