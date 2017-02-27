package org.dianahep.sparkrootapplications.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.math._

//import org.apache.spark.implicits._

import org.dianahep.sparkroot._


import org.dianahep.histogrammar._
import org.dianahep.histogrammar.ascii._

object BaconExampleApp {
  case class Photon(pt: Float, eta: Float, phi: Float,
    scEt: Float, scEta: Float, scPhi: Float, trkIso: Float,
    ecalIso: Float, hcalIso: Float, chHadIso: Float, gammaIso: Float,
    neuHadIso: Float, mva: Float, hovere: Float, sthovere: Float,
    sieie: Float, sipip: Float, r9: Float, fiducialBits: Int,
    typeBits: Int, scID: Int, hasPixelSeed: Boolean, 
    passElectronVeto: Boolean, isConv: Boolean, hltMatchBits: Seq[Boolean])
  case class GenParticle(parent: Int, pdfId: Int, status: Int,
    pt: Float, eta: Float, phi: Float, mass: Float, y: Float)
  case class Event(genParticle: Seq[GenParticle], 
    photon: Seq[Photon]);

  def main(args: Array[String]) {
    val inputFileName = args(0)
    val conf = new SparkConf().setAppName("Bacon Example Application")
    val spark = SparkSession.builder()
      .master("local")
      .appName("Bacon Example Application")
      .getOrCreate()

    doWork(spark, inputFileName)
    spark.stop()
  }

  def doWork(spark: SparkSession, inputName: String) = {
    // load the ROOT file
    val df = spark.sqlContext.read.root(inputName)

    // initialize the histograms
    val emptyBundle = Label(
      "pt" -> Bin(200, 0, 200, {event: Event => event.photon(0).pt}),
      "phi" -> Bin(200, -3.15, 3.15, {event: Event => event.photon(0).phi}),
      "eta" -> Bin(200, -2.5, 2.5, {event: Event => event.photon(0).eta})
    )
    
    // see https://issues.apache.org/jira/browse/SPARK-13540
    import spark.implicits._

    // build the RDD out of the Dataset and filter out right away
    val rdd = df.select("GenParticle", "Photon").as[Event].filter(_.photon.size!=0).rdd

    // process and aggregate the results
    val filledBundle = rdd.aggregate(emptyBundle)(new Increment, new Combine)


    // cute printing
    filledBundle("pt").println
    filledBundle("phi").println
    filledBundle("eta").println

    // save a plot
    /*
    val plot_pt = filledBundle("pt").bokeh().plot
    save(plot_pt, "photon_pt.html")
    val plot_eta = filledBundle("eta").bokeh().plot
    save(plot_eta, "photon_eta.html")
    val plot_phi = filledBundle("phi").bokeh().plot
    save(plot_phi, "photon_phi.html")
    */
  }
}
