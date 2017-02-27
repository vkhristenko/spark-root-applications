package org.dianahep.sparkrootapplications.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.math._

//import org.apache.spark.implicits._

import org.dianahep.sparkroot._
//import org.dianahep.sparkrootapplications.defs.aodpublic._

import org.dianahep.histogrammar._
import org.dianahep.histogrammar.ascii._

object AODPublicApp {
  def main(args: Array[String]) {
    val inputPath = args(0)
    val outPath = args(1)
    val spark = SparkSession.builder()
      .appName("AOD Public DS Example")
      .getOrCreate()

    doWork(spark, inputPath, outPath)
    spark.stop()
  }

  def doWork(spark: SparkSession, inputPath: String, outPath: String) = {
    val df = spark.sqlContext.read.option("tree", "Events").root(inputPath)
    println(s"Number of Rows = ${df.count}")

    // initialize the histograms
    /*
    val emptyBundle = Label(
      "muon_pt" -> Bin(200, 0, 200, {muon:  => event.muons(0)..pt}),
      "muon_phi" -> Bin(200, -3.15, 3.15, {event: Event => event.muons(0).baseTrack.phi}),
      "muon_eta" -> Bin(200, -2.5, 2.5, {event: Event => event.muons(0).baseTrack.eta})
    )*/
    
    // see https://issues.apache.org/jira/browse/SPARK-13540
    import spark.implicits._

    // build the RDD out of the Dataset and filter out right away
    //val rdd = df.select("Muons", "Electrons").as[Event].filter(_.electrons.size!=0).rdd

    // process and aggregate the results
    //val filledBundle = rdd.aggregate(emptyBundle)(new Increment, new Combine)
  /*

    // cute printing
    filledBundle("electron_pt").println
    filledBundle("muon_pt").println
    filledBundle("dimuon_mass").println
    filledBundle("muon_phi").println
    filledBundle("muon_eta").println

    // save a plot
    val plot_e = filledBundle("electron_pt").bokeh().plot()
    save(plot_e, "electron_pt.html")
    val plot_mu = filledBundle("muon_pt").bokeh().plot
    save(plot_mu, "muon_pt.html")
    val plot_dimuonmass = filledBundle("dimuon_mass").bokeh().plot
    save(plot_dimuonmass, "dimuon_mass.html")
    val plot_muoneta = filledBundle("muon_eta").bokeh().plot
    save(plot_muoneta, "muon_eta.html")
    val plot_muonphi = filledBundle("muon_phi").bokeh().plot
    save(plot_muonphi, "muon_phi.html")
    */
  }
}
