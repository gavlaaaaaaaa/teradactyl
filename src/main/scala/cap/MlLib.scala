package com.cap
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import java.util.Properties
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.mllib.clustering.StreamingKMeans
import java.security.MessageDigest 
/**
  * Created by vishal on 22/11/16.
  */
object mllib {

  def main(args: Array[String]): Unit ={

    val conf=new SparkConf().setMaster("local").setAppName("Bye");
    //val sc=new SparkContext(conf)
    val ssc = new StreamingContext(conf, Seconds(2))
    val Array(brokers, topics) = args

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,"auto.offset.reset" -> "smallest")    
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    messages.print() 
    val newmsg = messages.map( kafka => kafka._2.split('|')).filter(x => x.length > 16).map(data => (generateSHA(), data(0), Vectors.dense(data.slice(1,11).map(x => x.map(y=>y.toInt).mkString("").toDouble)), data(12), data(13), data(14),data(15),data(16),data(1)))
    val model = new StreamingKMeans().setK(3).setDecayFactor(1.0).setRandomCenters(10, 0.0)
    model.trainOn(newmsg.map(x => x._3))

    val output = model.predictOnValues(newmsg.map(x => ((x._1,x._2,x._5,x._6,x._7,x._8,x._9),x._3))).map(tup => (tup._1._1,tup._1._2,tup._1._3,tup._1._4, tup._1._5,tup._1._6,tup._1._7,tup._2))

    output.foreachRDD{ rdd =>
	val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
	import sqlContext.implicits._
	val group_id= rdd.toDF("rdd_id", "id", "change_id", "change_page", "change_type", "change_value", "timestamp", "cluster_group")
	val host_port = "jdbc:mysql://127.0.0.1:3306/test"

        val prop = new Properties()
        prop.setProperty("user", "lewis")
        prop.setProperty("password", "password")
        prop.setProperty("useSSL", "false")
        prop.setProperty("driver", "com.mysql.jdbc.Driver")
        group_id.write.mode("append").jdbc({host_port}, "group_data", prop)
    }
    

//    id, timestamp, event type, time spent on event, browser, ip, country, language, device, url, screen resolution, interaction, goal_id, change_id, change_page, change_type, change_value


//    data.collect().foreach(println)
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
    /*
    import sqlContext.implicits._
    val data = sc.textFile("test/data.txt").map(x => x.split("\t")).zipWithUniqueId().map(line => (line._2, line._1(0), line._1.slice(0,11), line._1(12), line._1(13))).toDF("rdd_id", "id", "words", "goal", "changes")

    val word2vec = new Word2Vec().setInputCol("words").setOutputCol("points").setVectorSize(11).setMinCount(0)
    val word_model = word2vec.fit(data)
    val vec = word_model.transform(data)

    val kmeans = new org.apache.spark.ml.clustering.KMeans().setK(3).setFeaturesCol("points").setPredictionCol("predict")
    val model = kmeans.fit(vec)
    val points = vec.select("rdd_id", "points").map(x => (x(0).asInstanceOf[Long], x(1).asInstanceOf[org.apache.spark.mllib.linalg.Vector]))
    
    val predict_group = new KMeansModel(model.clusterCenters).predict(points.map(_._2))
    val comb = points.map(_._1).zip(predict_group)
    
    val groupDF = comb.toDF("rdd_id", "cluster_group")
    val new_DF = data.join(groupDF, "rdd_id")
    val group_id = new_DF.select("rdd_id", "id", "cluster_group", "changes")
    val host_port = "jdbc:mysql://127.0.0.1:3306/test"

    val prop = new Properties()
    prop.setProperty("user", "lewis")
    prop.setProperty("password", "password")
    prop.setProperty("useSSL", "false")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    group_id.write.mode("append").jdbc({host_port}, "group_data", prop)
    */
 }

def generateSHA() : String = {
	val digest = MessageDigest.getInstance("SHA-256")
	digest.update(java.util.UUID.randomUUID().toString.getBytes())
	digest.update(System.nanoTime().toString.getBytes())
	return digest.digest().map("%02x".format(_)).mkString
}

}

