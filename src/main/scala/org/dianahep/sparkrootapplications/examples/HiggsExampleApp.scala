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

object HiggsExampleApp {
  case class Object();
  case class Track(obj: Object, charge: Int, pt: Float, pterr: Float, eta: Float, phi: Float);
  case class Electron(baseTrack: Track, ids: Seq[Boolean], sumChargedHadronPt: Float, sumNeutralHadronEt: Float, sumPhotonEt: Float, sumPUPt: Float, sumChargedParticlePt: Float, dz: Double, isPF: Boolean, convVeto: Boolean);
  case class Muon(baseTrack: Track, isTracker: Boolean, isStandAlone: Boolean, 
    isGlobal: Boolean, isTight: Boolean, isMedium: Boolean, isLoose: Boolean,
    isPF: Boolean, normChi2: Float, d0BS: Float, dzBS: Float, d0PV: Float,
    dzPV: Float, nPLs: Int, nTLs: Int, nSLs: Int, vfrTrk: Float, nvMHIts: Int,
    nvPHits: Int, nvTHits: Int, nvSHits: Int, nSegMts: Int,
    nMtsStatios: Int, trackIsoSumPt: Float, trackIsoSumPtCorr: Float,
    hIso: Float, eIso: Float, relCombIso: Float, track: Track, 
    segmentCompatiblity: Float, combinedQChi2: Float, combinedQTrkK: Float,
    isHLTMatched: Seq[Boolean], sumChargedHadronPt: Float,
    sumChargedParticlePt: Float, sumNeutralHadronEt: Float,
    sumPhotonEt: Float, sumPUPt: Float, sumChargedHadronPtR04: Float,
    sumChargedParticlePtR04: Float, sumNeutralHadronEtR04: Float, 
    sumPhotonEtR04: Float, sumPUPtR04: Float
    )
  case class Event(muons: Seq[Muon], electrons: Seq[Electron]);

  def main(args: Array[String]) {
    val inputFileName = args(0)
    val conf = new SparkConf().setAppName("Higgs Example Application")
    val spark = SparkSession.builder()
      .master("local")
      .appName("Higgs Example Application")
      .getOrCreate()

    doWork(spark, inputFileName)
    spark.stop()
  }

  def mass(mu1: Muon, mu2: Muon) = {
    val px1 =  mu1.baseTrack.pt*cos(mu1.baseTrack.phi)
    val px2 =  mu2.baseTrack.pt*cos(mu2.baseTrack.phi)

    val py1 =  mu1.baseTrack.pt*sin(mu1.baseTrack.phi)
    val py2 =  mu2.baseTrack.pt*sin(mu2.baseTrack.phi)

    val pz1 =  mu1.baseTrack.pt*sinh(mu1.baseTrack.eta)
    val pz2 =  mu2.baseTrack.pt*sinh(mu2.baseTrack.eta)

    val m = 0.105658367

    val energy1 = (px1*px1 + py1*py1 + pz1*pz1 + m*m)
    val energy2 = (px2*px2 + py2*py2 + pz2*pz2 + m*m)

    sqrt((energy1+energy2)*(energy1+energy2) - (px1+px2)*(px1+px2) - (py1+py2)*(py1+py2) -
      (pz1+pz2)*(pz1+pz2))
  }

  def doWork(spark: SparkSession, inputName: String) = {
    // load the ROOT file
    val df = spark.sqlContext.read.root(inputName)

    // initialize the histograms
    val emptyBundle = Label(
      "electron_pt" -> Bin(200, 0, 200, 
        {event: Event => event.electrons(0).baseTrack.pt}),
      "muon_pt" -> Bin(200, 0, 200, {event: Event => event.muons(0).baseTrack.pt}),
      "dimuon_mass" -> Bin(200, 0, 200, {event: Event => 
        mass(event.muons(0), event.muons(1))}),
      "muon_phi" -> Bin(200, -3.15, 3.15, {event: Event => event.muons(0).baseTrack.phi}),
      "muon_eta" -> Bin(200, -2.5, 2.5, {event: Event => event.muons(0).baseTrack.eta})
    )
    
    // see https://issues.apache.org/jira/browse/SPARK-13540
    import spark.implicits._

    // build the RDD out of the Dataset and filter out right away
    val rdd = df.select("Muons", "Electrons").as[Event].filter(_.electrons.size!=0).rdd

    // process and aggregate the results
    val filledBundle = rdd.aggregate(emptyBundle)(new Increment, new Combine)


    // cute printing
    filledBundle("electron_pt").println
    filledBundle("muon_pt").println
    filledBundle("dimuon_mass").println
    filledBundle("muon_phi").println
    filledBundle("muon_eta").println

    // save a plot
    /*
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
