/**
 * @author Chandresh Bhatt
 * 
 * KPI1_3:
 * 				Filter all the records with event_id = 100
 * 					--> Total No. of devices with channel type = "LiveTVMediaChannel"
 * Following is the Output : 
 * 					--> Total No. of Devices with ChannelType : LiveTVMediaChannel : 662
 * Verification : 
 *  				--> Total No. of Devices with any ChannelType : 693
 *  				--> Total No. of Devices with ChannelType : PPVTVMediaChannel : 1
 *  				--> Total No. of Devices with ChannelType : NEWLIVE : 1
 *  				--> Total No. of Devices with ChannelType : DvrAppMediaChannel : 25
 *  				--> Total No. of Devices with ChannelType : UrlMediaChannel : 4
 *  				Total (693) =  LiveTVMediaChannel(662) + PPVTVMediaChannel(1)  + NEWLIVE(1) + DvrAppMediaChannel(25) + UrlMediaChannel(4)   				
 */

package com.practice.dataflair.stb

import org.apache.spark.sql.SparkSession

object SetTopBox_KPI1_3 {
  
  def main(args: Array[String]): Unit = {
    if(args.length < 2) {
      System.err.println("SetTopBox Analysis <Input-file> OR <Output-file> is missing")
      System.exit(1)
    }
    
    val spark = SparkSession.builder().appName("KPI1_3").getOrCreate()
    val record = spark.read.textFile(args(0)).rdd
    val eventid_100 = record.filter {
                                      filterlines_100 => filterlines_100.contains("^100^") 
                                    }
                                    .map {
                                      x => x.replace("^", "#")
                                    }
                                    
    val onlyxmltags = eventid_100.flatMap { x => x.split("#").drop(4).dropRight(2) }
    
    val xmlstring = onlyxmltags.map { x => x.toString().trim() }
    
    val loadxml = xmlstring.map { x => xml.XML.loadString(x) }
    
    val attributewithnv = loadxml.map { x => x.\\("nv") }                                      
    
    val nodeschanneltype = attributewithnv.map { x => x.theSeq(x.length-2) }
                                          .map { x => x \ "@v"} 
                           
    val livetvmediachannel = nodeschanneltype.filter { x => (x.text.contains("LiveTVMediaChannel"))} 
     
    println("Total No. of Device with ChannelType : LiveTVMediaChannel : " + livetvmediachannel.count())
                                          
    livetvmediachannel.saveAsTextFile(args(1))
    spark.stop()
  }
}

//bin/spark-submit --class com.practice.dataflair.stb.SetTopBox_KPI1_3 ../stb1_3.jar ../Set_Top_Box_Data.txt stb1_3-out-001
//bin/spark-submit --class com.practice.dataflair.stb.SetTopBox_KPI2 ../stb2.jar ../Set_Top_Box_Data.txt stb02-out-001