//Importing all the packages that are necessary for the program execution//
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import scala.collection.mutable.ArrayBuffer

//Defining Class and the main method//

object Citibikeanalysis {
  def main(args: Array[String]){
  
//Creating SparkContext and SQL Context//   
  
    val sparkConf = new SparkConf().setAppName("CitiBikeAnalysis")
    val sc = new SparkContext(sparkConf)
    val spark = new SQLContext(sc)
  
    //Importing spark implicits for dataframe and dataset//
    
    import spark.implicits._

//Getting the data in to Dataframe and registering it in a temporay table //
    
    val data = spark.read.format("com.databricks.spark.csv").option("header","true").option("delimiter",",").option("inferSchema","true").load("/user/mrstephyjacob4610/Spark/Citi_Bike_Trip_Data")
    data.registerTempTable("DataTable")
    
//Filtering the data having teh same station and and destination mentioned only to find out the frequent ride route with the given data//
    
    val datafilter = data.filter(col("start station id") !== col("end station id"))
    datafilter.registerTempTable("datafiltertable")
    
 //Finding the maximum ride route and printing it //
    
    val max_ride_route = spark.sql("select `start station id`,`start station name`,`end station id`,`end station name`,count(*) from datafiltertable group by `start station id`,`start station name`,`end station id`,`end station name` ORDER BY 5 DESC").select("start station name","end station name").take(1)
    println("The route in which Citi Bikers ride most are with start and end destination displayed consecutively as below:")
    max_ride_route.foreach { row => row.toSeq.foreach{col => println(col)}}
 
 //Finding the Biggest Trip based on the duration and then printing start station, end station,duration,start time and end time of it seperately //   
    
    val bigtrip = spark.sql("select * from DataTable order by tripduration desc limit 1")
    val bigstartstat = bigtrip.select("start station name").collect.mkString.replaceAll("[\\[\\]]","")
    println("Biggest trip start station is "+bigstartstat)
    val bigstartend = bigtrip.select("end station name").collect.mkString.replaceAll("[\\[\\]]","")
    println("Biggest trip end station is "+bigstartend)
    val bigtripduration = bigtrip.select("tripduration").collect.mkString.replaceAll("[\\[\\]]","")
    println("Biggest trip duration is "+bigtripduration+" seconds")
    val bigtripstarttime = bigtrip.select("starttime").collect.mkString.replaceAll("[\\[\\]]","")
    println("Biggest Trip start time was on : "+bigtripstarttime)
    val bigtripendtime = bigtrip.select("stoptime").collect.mkString.replaceAll("[\\[\\]]","")
    println("Biggest Trip end time was on : "+bigtripendtime)
    
 //Finding the distance between each station for all records using Haversine Formula// 
    
    val earth_radius = 6371
    val latitudes = datafilter.select("start station latitude","end station latitude").map{case Row(slat: Double,elat: Double) => (slat,elat)}
    val longitudes = datafilter.select("start station longitude","end station longitude").map{case Row(slong: Double, elong:Double) => (slong,elong)}  
    val distanceinkm = new ArrayBuffer[Int]              //Defining an ArrayBuffer// 
    for (i <- 0 to datafilter.count.toInt-1){            //Iterating Latitudes and Longitudes through a for loop//  
	    var latDistance = Math.toRadians((latitudes.collect()(i)._1) - (latitudes.collect()(i)._2))
	    var lonDistance = Math.toRadians((longitudes.collect()(i)._1) - (longitudes.collect()(i)._2))
	    var sinLat = Math.sin(latDistance / 2)
	    var sinLng = Math.sin(lonDistance / 2)
	    var a = sinLat * sinLat + (Math.cos(Math.toRadians(latitudes.collect()(i)._1)) * Math.cos(Math.toRadians(latitudes.collect()(i)._2)) * sinLng * sinLng)
	    var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
	    distanceinkm.append((earth_radius*c).toInt)        //Inserting each value in to an ArrayBuffer//
	  }
    
    val distancinkmrdd = sc.parallelize(distanceinkm)    //Converting the ArrayBuffer collection to a RDD//
    val rdd_p = distancinkmrdd.coalesce(datafilter.rdd.getNumPartitions)      //Creating another RDD for distanceinkmrdd with the same partitions as of the datafilter rdd inorder to zip both RDDs//
    val rdd_new = datafilter.rdd.zip(rdd_p).map(r => Row.fromSeq(r._1.toSeq ++ Seq(r._2)))    //Zipping both RDDs and creating a new RDD//
    val datawithdistancedf = spark.createDataFrame(rdd_new,datafilter.schema.add("DistanceinKm", IntegerType)) //Creating a new Dataframe with DistanceinKm as a new column to the rdd_new data//
    
 //Displaying with DistanceinKm for the entire data//
    
    println("Displaying Tripduration,Start station and End station with DistanceinKM for the entire data by calculating distanceinkm using Havresine Forumala is as below:\n")
    datawithdistancedf.select("tripduration","start station name","end station name","DistanceinKm").orderBy(desc("DistanceinKm")).show(datawithdistancedf.count.toInt,false)

//Finding the start station and end station of the longest distance traveled trip//
    
    val longestdistancetrip = datawithdistancedf.select("tripduration","start station name","end station name","	DistanceinKm").orderBy(desc("DistanceinKm")).take(1)
    val longestdistancestations = longestdistancetrip.map{case Row(duration: Int,startStat: String, endstat: String, distance: Int) => (startStat,endstat)}
    println("Start station and End station of the longest trip based on distance travelled are : "+longestdistancestations(0)._1+" and "+longestdistancestations(0)._2)
    
//Finding 10 most popular stations from 5 most popular start station and 5 most popular end station and printing it//
    
    val mpstartstat = spark.sql("select `start station name`,count(*) from DataTable GROUP BY `start station name` ORDER BY 2 DESC LIMIT 5").select("start station name").collect.mkString("\n").replaceAll("[\\[\\]]","")
    val mpendstat = spark.sql("select `end station name`,count(*) from DataTable GROUP BY `end station name` ORDER BY 2 DESC LIMIT 5").select("end station name").collect.mkString("\n").replaceAll("[\\[\\]]","")
    println("Most 10 popular stations are: \n"+mpstartstat+mpendstat)

 //Finding the day of the week which has most rides using Spark SQL function dayofweek after casting String variable of starttime in to timestamp//
    
    val days = Map(1 -> "Sunday", 2 -> "Monday", 3 -> "Tuesday", 4 -> "Wednesday", 5 -> "Thursday", 6 -> "Friday", 7 -> "Saturday")
    val MostRideDay = spark.sql("SELECT DAYOFWEEK(CAST(starttime as TIMESTAMP)) as Day,count(*) as Rides FROM DataTable GROUP BY Day ORDER BY 2 DESC LIMIT 1").map{case Row(day: Int, rides: Long) => (day,rides)}
    println("Most rides where taken on "+days(MostRideDay.collect()(0)._1)+" and the total ride taken are: "+MostRideDay.collect()(0)._2)  
}
}