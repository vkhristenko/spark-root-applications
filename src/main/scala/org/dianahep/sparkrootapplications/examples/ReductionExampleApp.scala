package org.dianahep.sparkrootapplications.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

import scala.math._

//import org.apache.spark.implicits._

import org.dianahep.sparkroot._
import org.dianahep.sparkrootapplications.defs.aodpublic._

import org.dianahep.histogrammar._
import org.dianahep.histogrammar.ascii._

object ReductionExampleApp {
  case class Event(muons: Seq[RecoLeafCandidate]);
  def main(args: Array[String]) {
    val inputPath = args(0)
    val spark = SparkSession.builder()
      .appName("AOD Public DS Example")
      .getOrCreate()

    // get the Dataset[Row] = Dataframe (from 2.0)
    val df = spark.sqlContext.read.option("tree", "Events").root(inputPath)

    // at this point at least...
    import spark.implicits._

    // select only AOD Collection of Muons. Now you have Dataset[Event].
    val dsMuons = df.select("recoMuons_muons__RECO_.recoMuons_muons__RECO_obj.reco::RecoCandidate.reco::LeafCandidate").toDF("muons").as[Event]

    // define the empty[zero] container of Histos
    val emptyDiCandidate = Label(
      "pt" -> Bin(200, 0, 200, {m: DiCandidate => m.pt}),
      "eta" -> Bin(48, -2.4, 2.4, {m: DiCandidate => m.eta}),
      "phi" -> Bin(63, -3.15, 3.15, {m: DiCandidate => m.phi}),
      "mass" -> Bin(200, 0, 200, {m: DiCandidate => m.mass})
    )

    val dsDiMuons = dsMuons.filter(_.muons.length>=2)
      .flatMap({e: Event => for (i <- 0 until e.muons.length; j <- 0 until e.muons.length) yield buildDiCandidate(e.muons(i), e.muons(j))})
    dsDiMuons.show
    
    // get current user name
    val userName = System.getProperty("user.name")

    // get current date and time
    val now = Calendar.getInstance().getTime();
    val dateFormatter = new SimpleDateFormat("YYMMdd_HHmmss");
    val date = dateFormatter.format(now)

    // create filenames
    val parquetFilename = "file:/tmp/" + userName + "_" + date + "_testReduced.parquet"
    val jsonFilename = "/tmp/" + userName + "_" + date + "_testBundle.json"

    dsDiMuons.write.format("parquet").save(parquetFilename)
    val filled = dsDiMuons.rdd.aggregate(emptyDiCandidate)(new Increment, new Combine);
    filled("pt").println;
    filled.toJsonFile(jsonFilename)

    // stop the session/context
    spark.stop
  }
}
