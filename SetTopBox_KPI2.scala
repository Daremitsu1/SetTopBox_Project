/**
 * @author chandresh bhatt
 * 
 * KPI 2 : 
 *  -> Filter all the records with Event ID = 101 
 *  	-> Get the total no. of devices with Power State =  "ON" or Power State = "OFF"  
 *  
 *  Following is the Output : 
 *  	## Record with Event ID = 101, Devices with PowerState= ON : 547, Devices with PowerState = OFF : 453 ##
 *  
 */

package com.practice.dataflair.stb

import org.apache.spark.sql.SparkSession

object SetTopBox_KPI2 {
  
  def main(args: Array[String]): Unit = {
           
    //System.setProperty("hadoop.home.dir", "C:\\Users\\chandresh bhatt\\Desktop\\DataFlair\\Hadoop Tar\\hadoop-2.5.0-cdh5.3.2");
    if(args.length < 2) {
      System.err.println("SetTopBox Data Analysis <Input-File> OR <Output-File> is missing")
      System.exit(1)
    }
    
    //val spark = SparkSession.builder().appName("KPI2").master("local").getOrCreate()
    val spark = SparkSession.builder().appName("KPI2").getOrCreate()
    
    //val record = spark.read.text("C:\\Users\\chandresh bhatt\\Desktop\\Spark\\SetTopBox_SparkProject\\Set_Top_Box_Data.txt").rdd
    val record = spark.read.text(args(0)).rdd
    
    val eventid_101 = record.filter { 
                                      findeventid_101 => findeventid_101.getString(0).contains("^101^")
                                    }
                                    .map
                                    { 
                                      replacespace => replacespace.getString(0).replace("^", "#")
                                    } 
    
    val xmltags = eventid_101.flatMap {
                                            xmltagsonly => xmltagsonly.split("#").drop(4).dropRight(2)
                                           }
    
    val removespacefromxml = xmltags.map { xml_string => xml_string.toString().trim() }
            
    val xmlfile = removespacefromxml.map { x => xml.XML.loadString(x) } //xml.XML.loadString(arrayelement2.toString()) }
    
    val xmlattributeswithvalue = xmlfile.map { x => x.\\("nv")}
        
    val onlypowerstateattribute = xmlattributeswithvalue.map { x => x.theSeq(1) }
    
    val powerstateonoroff = onlypowerstateattribute.map { x => x \\ "nv" \ "@v" }
  
    val pon = powerstateonoroff.filter { x => x.text.equals("ON") }
    val poff = powerstateonoroff.filter { x => x.text.equals("OFF") }
    
    println("Record with Event ID = 101, Devices with PowerState= ON : " + pon.count() + ", Devices with PowerState = OFF : " + poff.count())
                                          
    spark.stop()
  }
}
//---------------------------------------------------------------------------------------------------------------------------------------------------------
//bin/spark-submit --class com.practice.dataflair.stb.SetTopBox_KPI2 ../stb2.jar ../Set_Top_Box_Data.txt stb02-out-001
//val formatdata2 = record.map { x => x.mkString.replace("^", " ")}  replace ^ with space
//---------------------------------------------------------------------------------------------------------------------------------------------------------

//---------------------------------------------------------------------------------------------------------------------------------------------------------
//val arrayelement2 = arrayelement.flatMap { x => x.split(">")}
    //val arrayelement3 = arrayelement2.map { 
    //                                          x => (x.toString() + ">").trim() 
    //                                      }
    
    
    //val xmlfile = ("<" + "?xml version=" + "\"1.0\"" + " " + "encoding=" + "\"UTF-8\"" + "?" + ">" + "\n" + arrayelement3.toString())                                                                              
                                         
        
    //val xmlfile1 = xml.XML.loadString(arrayelement3.toString())
     
            
    
    
    
    //println(xml_tags.count { x => (x.attributes.key == "POWERON" && x.attributes.value == "ON")  })
    
    //arrayelement.saveAsTextFile(args(1))
    //eventid_101.saveAsTextFile(args(1))
//---------------------------------------------------------------------------------------------------------------------------------------------------------
