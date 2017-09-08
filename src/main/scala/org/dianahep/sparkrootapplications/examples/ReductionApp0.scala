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
import org.dianahep.sparkrootapplications.defs.cmssw._

import org.dianahep.histogrammar._
import org.dianahep.histogrammar.ascii._

object ReductionApp0 {
  case class Event(
    muons: Seq[RecoLeafCandidate],
    photons: Seq[RecoLeafCandidate],
    electrons: Seq[RecoLeafCandidate],
    jets: Seq[RecoLeafCandidate],
    met: Seq[RecoLeafCandidate]
  );
  case class EventStripped(
    muons: Seq[RecoLeafCandidateStripped],
    photons: Seq[RecoLeafCandidateStripped],
    electrons: Seq[RecoLeafCandidateStripped],
    jets: Seq[RecoLeafCandidateStripped],
    met: Seq[RecoLeafCandidateStripped]
  );

  def main(args: Array[String]) {
    if (args.size<2)
      return;
    val inputPath = args(0)
    val outputPathName = args(1)
    val spark = SparkSession.builder()
      .appName("AOD Public Dataset: Reduction App0")
      .getOrCreate()

    // get the Dataset[Row] = Dataframe (from 2.0)
    val df = spark.sqlContext.read.option("tree", "Events").root(inputPath)

    // at this point at least...
    import spark.implicits._

    // select Muons, Photons, Electrons, Jets, MET - all the main physics objects
    val dsEvents = df.select(
      "recoMuons_muons__RECO_.recoMuons_muons__RECO_obj.reco::RecoCandidate.reco::LeafCandidate",
      "recoPhotons_photons__RECO_.recoPhotons_photons__RECO_obj.reco::RecoCandidate.reco::LeafCandidate",
      "recoGsfElectrons_gsfElectrons__RECO_.recoGsfElectrons_gsfElectrons__RECO_obj.reco::RecoCandidate.reco::LeafCandidate",
      "recoPFJets_ak5PFJets__RECO_.recoPFJets_ak5PFJets__RECO_obj.reco::Jet.reco::CompositePtrCandidate.reco::LeafCandidate",
      "recoMETs_htMetAK5__RECO_.recoMETs_htMetAK5__RECO_obj.reco::RecoCandidate.reco::LeafCandidate"
     ).toDF("muons", "photons", "electrons", "jets", "met").as[Event];

    // apply the Selections of some sort
    val dsStripped = dsEvents
      .filter(_.muons.length>=2)
      .map({ // this mapping is obligatory for parquet writing...
        e: Event => EventStripped(
          e.muons.map(RecoLeafCandidateStripped(_)),
          e.photons.map(RecoLeafCandidateStripped(_)),
          e.electrons.map(RecoLeafCandidateStripped(_)),
          e.jets.map(RecoLeafCandidateStripped(_)),
          e.met.map(RecoLeafCandidateStripped(_))
        )
      })
//      .filter(_.photons.length>0)

    dsStripped.write.parquet(outputPathName)

    // stop the session/context
    spark.stop
  }
}
