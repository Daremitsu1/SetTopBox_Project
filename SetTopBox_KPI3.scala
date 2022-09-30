/**
 * @author Chandresh Bhatt
 * 
 * KPI3 :
 * 		--> Filter all the records with EventID = 102 /113
 * 			--> Get the maximum price group by offer id
 * 
 * 
 * Output : [Excerpt from Output file]
 * (f3fd1eee-9608-4bcd-8e58-c0ae611f7f5b,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (7a26186d-2fb6-45cd-b91b-969b918f3261,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (3a90fc66-9e5f-4edb-8fea-3bbc1d438a88,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (0dff6733-0b76-4bc5-b418-9862f7aa8eb6,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (9bb3e4b5-b34a-4e25-9afb-c6eecd162ea8,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (b6cfb3b9-58a4-481a-b3d7-ae91cae0e009,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (84dbd346-304c-41e5-8439-85aa5930bfac,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (01ca6709-f8cb-43ec-99cd-d937467a9bad,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (4b8e91dc-6889-41de-8a86-72d30cc8d340,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (207dacc1-f28f-4655-b417-e22ca5f4f3a5,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (e17053c1-a155-45df-9f78-dd43aae7c399,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (be866b9b-42b7-4477-8c94-11d707a73ae0,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (153acb82-e221-454e-9426-e3ae19a1fe8e,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (d37eb754-9a24-440b-8f87-174b60d0f91e,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (e0851921-5e11-470b-829c-010c9e1574e1,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (70fd221d-6392-4d82-8a95-639cd4ddb75d,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (dbf0ca1e-8e09-46cd-9120-9e868a642ff9,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (5aeb11aa-0d1c-4f03-acd0-a07fd17adbc7,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (51759aac-1a35-4a9c-8e7a-d72c07520139,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (c394cf26-a441-4636-8e76-0e8897c1acfe,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (51b1983e-42d0-487a-a8b7-7e9900402d98,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (45b81190-2e10-4627-a51f-84a94e686a30,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (f54d4a57-9394-451c-af79-d03db5ab77ad,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (e62a6089-25aa-4614-8524-4a05a47099b4,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (00020fa2-d0b7-4a4a-b010-b9e204297b4e,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0, 60.0))
* (b7366456-96d4-4b7f-afb3-a1185d58fb24,CompactBuffer(60.0, 60.0, 60.0, 60.0, 60.0))
* (999c1871-200c-427a-84bf-1139c36f192f,CompactBuffer(60.0))
* (c0d1ac87-a22a-4345-847e-689d97614b96,CompactBuffer(50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0, 50.0))
 * 
 */

package com.practice.dataflair.stb

import org.apache.spark.sql.SparkSession

object SetTopBox_KPI3 {
 
  def main(args: Array[String]): Unit = {
    if(args.length < 2){
      System.err.println("SetTopBox Data Analysis <Input-File> OR <Output-File> is missing")
      System.exit(1)
    }
    
    val spark = SparkSession.builder().appName("KPI3").getOrCreate()
    
    val record = spark.read.textFile(args(0)).rdd
    
    val eventid_102_113 = record.filter { x => (x.contains("^102^") || x.contains("^113^")) }
                                                         .map { x => x.replace("^", "#") }
    
     val xmltags = eventid_102_113.flatMap { x => x.split("#").drop(4).dropRight(2) }
     
     val removespace = xmltags.map { x => x.toString().trim() }
    
     val strtoxml = removespace.map { x => xml.XML.loadString(x) }
     
     val xmlattributes = strtoxml.map { x => x.\\("nv") }
     
     val tuple2_price_offerid = xmlattributes.map { x => (x.theSeq(3), x.theSeq(2)) }
     
     val tuple2_priceofferid_value = tuple2_price_offerid.map(x => ((x._1 \\ "nv" \"@v"), x._2 \\ "nv" \ "@v"))
     
     val removeemptyprice = tuple2_priceofferid_value.filter(x => (!(x._1.text.isEmpty())))
     
     val f2 = removeemptyprice.map(x => (x._1.text.toDouble, x._2.text.toString()))
     
     val maxpricegroupbyofferid = f2.sortBy(x => (x._1, x._2), false)
      
     val op1 = maxpricegroupbyofferid.map(x => (x._2, x._1))
     
     val op2 = op1.groupByKey().sortBy(x => x._2, false)
    
     op2.saveAsTextFile(args(1))   
    
     //val tuple2_maxpriceofferid = f1.sortByKey(false)
           
     //tuple2_maxpriceofferid.saveAsTextFile(args(1))
    
    spark.stop()
  }
}
//bin/spark-submit --class pkg.class jarfile inputfile outputfile
//bin/spark-submit --class  com.practice.dataflair.stb.SetTopBox_KPI3 ../stb3.jar ../Set_Top_Box_Data.txt stb3-out-001