package org.dianahep.sparkrootapplications.benchmarks

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

case class DatasetQuery[T](name: String,
                      query: Dataset[T] => Unit,
                      d: Dataset[T], n: Integer) {
  def run = {
    for (i <- 0 until n) {
      println(s"started running $name trial $i/$n")
      query(d)
      println(s"finished running $name trial $i/$n")
    }
  }
}

// for casting
case class Event(muons: Seq[RecoLeafCandidate]);

object AODPublicBenchmarkApp {
  def main(args: Array[String]) {
    val inputPath = args(0)
    val outPath = args(1)
    val numTrials = args(2).toInt
    val testsToRun = args(3).split('.').map(_.toInt)
    val spark = SparkSession.builder()
      .appName("AOD Public DS Example")
      .getOrCreate()

    // get the DF
    val df = spark.sqlContext.read.option("tree", "Events").root(inputPath)

    import spark.implicits._

    //
    // B1 - count on Dataframe=Dataset[Row]
    //
    val b1 = List(
      new DatasetQuery("df.count example",{d: Dataset[Row] => d.count}, df, numTrials)
    )

    //
    // B2 on Muons[pt]
    // 0 - flatMap + count
    // 1 - flatMap.filter.reduce
    //
    val dfMuons = df.select("recoMuons_muons__RECO_.recoMuons_muons__RECO_obj.reco::RecoCandidate.reco::LeafCandidate")
    val dfPt = dfMuons.select("reco::LeafCandidate.pt_").flatMap(_.getSeq[Float](0)).select($"value".alias("pt_")).as[Float]
    val b2 = List(
      new DatasetQuery("df.flatMap:dfPt.count", {d: Dataset[Float] => d.count}, 
        dfPt, numTrials),
      new DatasetQuery("dfPt.filter(pt_ < 10).reduce({(x:Float, y: Float) => x+y})",
        {d: Dataset[Float] => d.filter("pt_ < 10").reduce({(x:Float, y: Float) => x+y})},
        dfPt, numTrials)
    )

    //
    // B3 on Muons[pt, eta, phi]
    //
    def fff(x: ((Float, Float), Float)) = (x._1._1, x._1._2, x._2)
    val names = Seq("pt_", "phi_", "eta_")
    type TripleFloat = (Float, Float, Float);
    val dfPtEtaPhi = dfMuons.select("reco::LeafCandidate.pt_", 
      "reco::LeafCandidate.eta_", "reco::LeafCandidate.phi_").flatMap[(Float, Float, Float)]({row: Row => row.getSeq[Float](0).zip(row.getSeq[Float](1)).zip(row.getSeq[Float](2)).map(fff)}).toDF(names: _*).as[(Float, Float, Float)]
    dfPtEtaPhi.show;dfPtEtaPhi.printSchema
    val b3 = List(
      new DatasetQuery("""dfPtEtaPhi.filter("eta_ < 0").reduce({(x: (Float, Float, Float), y: (Float, Float, Float)) => (x._1+y._1, x._2+y._2, x._3+y._3)})""",
        {d: Dataset[TripleFloat] => d.filter("eta_ < 0").reduce({(x: (Float, Float, Float), y: (Float, Float, Float)) => (x._1+y._1, x._2+y._2, x._3+y._3)})}, dfPtEtaPhi, numTrials),
      new DatasetQuery("""dfPtEtaPhi.filter("phi_ > 0").reduce({(x: (Float, Float, Float), y: (Float, Float, Float)) => (x._1+y._1, x._2+y._2, x._3+y._3)})""",
        {d: Dataset[TripleFloat] => d.filter("phi_ > 0").reduce({(x: (Float, Float, Float), y: (Float, Float, Float)) => (x._1+y._1, x._2+y._2, x._3+y._3)})}, dfPtEtaPhi, numTrials),
      
      new DatasetQuery("""dfPtEtaPhi.filter("phi_ > 0").map({x: (Float, Float, Float) => sqrt(x._1)}).reduce({(x: Double, y: Double) => x+y})""",
        {d: Dataset[TripleFloat] => d.filter("phi_ > 0").map({x: (Float, Float, Float) => sqrt(x._1)}).reduce({(x: Double, y: Double) => x+y})}, dfPtEtaPhi, numTrials),

      new DatasetQuery("""dfPtEtaPhi.filter("phi_ > 0").map({x: (Float, Float, Float) => sqrt(x._1)}).describe("pt_", "phi_", "eta_").show""", 
        {d: Dataset[TripleFloat] => d.filter("phi_ > 0").map({x: (Float, Float, Float) => sqrt(x._1)}).toDF("something").describe("something").show}, dfPtEtaPhi, numTrials)
    )

    //
    // B4 Dataset[Event]
    // 
    def computeSomething(m: RecoLeafCandidate): Unit = {m.pt*m.pt;return}
    def computeSomething2(c: DiCandidate): Unit = {c.mass*c.e; return}
    def computeMass(c: DiCandidate): Float = c.e*c.e - c.px*c.px - c.py*c.py - c.pz*c.pz
    val dsEvents = dfMuons.toDF("muons").as[Event]
    val b4 = List(
      new DatasetQuery("""dsEvents.count""", {d: Dataset[Event] => d.count},
        dsEvents, numTrials),
      new DatasetQuery("""dsEvents.filter(_.muons.length>2).count""",
        {d: Dataset[Event] => d.filter(_.muons.length>2).count}, dsEvents, numTrials),
      new DatasetQuery("""dsEvents.filter(_.muons.length==1).count""",
        {d: Dataset[Event] => d.filter(_.muons.length==1).count}, dsEvents, numTrials),
      new DatasetQuery("""dsEvents.filter(_muons.length==2).count""",
        {d: Dataset[Event] => d.filter(_.muons.length==2).count}, dsEvents, numTrials),

      new DatasetQuery("""d.groupByKey({e: Event => e.muons.length}).count.map(_._2).as[Long].reduce({(x: Long, y: Long) => x+y})""",
      {d: Dataset[Event] => d.groupByKey({e: Event => e.muons.length})
        .count.map(_._2).as[Long]
        .reduce({(x: Long, y: Long) => x+y})}, dsEvents, numTrials),

      new DatasetQuery("""dsEvents.filter(_.muons.length==2).flatMap[RecoLeafCandidate](_.muons).count""", {d: Dataset[Event] => d.filter({e: Event => e.muons.length==2}).flatMap[RecoLeafCandidate]({e: Event => e.muons}).count}, dsEvents, numTrials),
      new DatasetQuery("""dsEvents.filter({e: Event => e.muons.length==2}).flatMap[RecoLeafCandidate]({e: Event => e.muons}).reduce({(x: RecoLeafCandidate, y: RecoLeafCandidate) => if (x.pt>y.pt) x else y})""", {d: Dataset[Event] => d.filter({e: Event => e.muons.length==2}).flatMap[RecoLeafCandidate]({e: Event => e.muons}).reduce({(x: RecoLeafCandidate, y: RecoLeafCandidate) => if (x.pt>y.pt) x else y})}, dsEvents, numTrials),
      new DatasetQuery("""dsEvents.filter({e: Event => e.muons.length==2}).flatMap[RecoLeafCandidate]({e: Event => e.muons}).foreach(computeSomething)""",
        {d: Dataset[Event] => d.filter({e: Event => e.muons.length==2}).flatMap[RecoLeafCandidate]({e: Event => e.muons}).foreach(computeSomething(_))}, dsEvents, numTrials),

      new DatasetQuery("""d.filter({e: Event => e.muons.length==2}).flatMap[RecoLeafCandidate]({e: Event => e.muons}).filter({m: RecoLeafCandidate => (m.pt > 10.0 && m.eta<2.4)}).foreach(computeSomething(_))""",
        {d: Dataset[Event] => d.filter({e: Event => e.muons.length==2})
          .flatMap[RecoLeafCandidate]({e: Event => e.muons})
          .filter({m: RecoLeafCandidate => (m.pt > 10.0 && m.eta<2.4)})
          .foreach(computeSomething(_))}, 
        dsEvents, numTrials),

      new DatasetQuery("""{d: Dataset[Event] => d.filter({e: Event => e.muons.length>=2}).map({e: Event => {val muons = e.muons.sortWith(_.pt > _.pt); buildDiCandidate(muons(0), muons(1))}}).reduce({(x: DiCandidate, y: DiCandidate) => if (x.mass>y.mass) x else y}).mass""",
        {d: Dataset[Event] => d.filter({e: Event => e.muons.length>=2})
          .map({e: Event => {
            val muons = e.muons.sortWith(_.pt > _.pt); 
            buildDiCandidate(muons(0), muons(1))}})
          .reduce({(x: DiCandidate, y: DiCandidate) => 
            if (x.mass>y.mass) x else y}).mass
        }, dsEvents, numTrials
      ),

      new DatasetQuery("""{d: Dataset[Event] => d.filter({e: Event => e.muons.length>=2}).map({e: Event => {val muons = e.muons.sortWith(_.pt > _.pt); buildDiCandidate(muons(0), muons(1))}}).foreach(computeSomething(_))""",
        {d: Dataset[Event] => d.filter({e: Event => e.muons.length>=2})
          .map({e: Event => {
            val muons = e.muons.sortWith(_.pt > _.pt); 
            buildDiCandidate(muons(0), muons(1))}})
          .foreach(computeSomething2(_))
        }, dsEvents, numTrials
      )
    );

    //
    // B5 Dataset[Event] + RDD[Event] + Histogrammar
    //
    val emptyCandidate = Label(
      "pt" -> Bin(200, 0, 200, {m: RecoLeafCandidate => m.pt}),
      "eta" -> Bin(48, -2.4, 2.4, {m: RecoLeafCandidate => m.eta}),
      "phi" -> Bin(63, -3.15, 3.15, {m: RecoLeafCandidate => m.phi})
    )
    val emptyDiCandidate = Label(
      "pt" -> Bin(200, 0, 200, {m: DiCandidate => m.pt}),
      "eta" -> Bin(48, -2.4, 2.4, {m: DiCandidate => m.eta}),
      "phi" -> Bin(63, -3.15, 3.15, {m: DiCandidate => m.phi}),
      "mass" -> Bin(200, 0, 200, {m: DiCandidate => m.mass})
    )

    val b5 = List(
      new DatasetQuery( """d.rdd.count""",
        {d: Dataset[Event] => d.rdd.count}, dsEvents, numTrials),
      new DatasetQuery("""d.rdd.filter(_.muons.length==2).count""",
        {d: Dataset[Event] => d.rdd.filter(_.muons.length==2).count},dsEvents, numTrials),

      new DatasetQuery("""d.rdd.groupBy({e: Event => e.muons.length}).map(_._2.count({e: Event => true})).reduce(_ + _)""",
        {d: Dataset[Event] => d.rdd.groupBy({e: Event => e.muons.length})
          .map(_._2.count({e: Event => true})).reduce(_ + _)}, dsEvents, numTrials),

      new DatasetQuery("""d.filter(_.muons.length > 0).flatMap(_.muons).rdd.aggregate(emptyCandidate)(new Increment, new Combine)""",
        {d: Dataset[Event] => {val h = d.filter(_.muons.length > 0)
          .flatMap(_.muons).rdd.aggregate(emptyCandidate)(new Increment, new Combine);
          h("pt").println; h.toJsonFile(if (outPath.last == '/') outPath ++ "bundleMuons__filter_gt0.json" else (outPath ++ "/" ++ "bundleMuons__filter_gt0.json"))}},
        dsEvents, numTrials),

      new DatasetQuery("""d.filter(_.muons.length == 2).flatMap(_.muons).rdd.aggregate(emptyCandidate)(new Increment, new Combine)""",
        {d: Dataset[Event] => { val h = d.filter(_.muons.length == 2)
          .flatMap(_.muons).rdd.aggregate(emptyCandidate)(new Increment, new Combine);
          h("pt").println; h.toJsonFile(if (outPath.last == '/') outPath ++ "bundleMuons__filter_eq2.json" else (outPath ++ "/" ++ "bundleMuons__filter_eq2.json"))}},
        dsEvents, numTrials),

      new DatasetQuery("""d.filter(_.muons.length >= 2).flatMap({e: Event => for (i <- 0 until e.muons.length; j <- 0 until e.muons.length) yield buildDiCandidate(e.muons(i), e.muons(j))}).rdd.aggregate(emptyDiCandidate)(new Increment, new Combine)""",
        {d: Dataset[Event] => {val h = d.filter(_.muons.length >= 2)
          .flatMap({e: Event => for (i <- 0 until e.muons.length; j <- 0 until e.muons.length) yield buildDiCandidate(e.muons(i), e.muons(j))})
          .rdd.aggregate(emptyDiCandidate)(new Increment, new Combine);
          h("pt").println; 
          h.toJsonFile(if (outPath.last == '/') outPath ++ "bundleDiMuons.json" 
          else (outPath ++ "/" ++ "bundleMuons.json"))}},
        dsEvents, numTrials)
    )

    //
    // B6 - Physics Related for Muons
    // 0 - filter.flatMap.aggregate 
    // 1 - filter.flatMap.aggregate
    //
    val b6 = List(
      new DatasetQuery("""d.filter(_.muons.length > 0).flatMap(_.muons).rdd.aggregate(emptyCandidate)(new Increment, new Combine)""",
        {d: Dataset[Event] => {val h = d.filter(_.muons.length > 0)
          .flatMap(_.muons).rdd.aggregate(emptyCandidate)(new Increment, new Combine);
          h("pt").println; h.toJsonFile(if (outPath.last == '/') outPath ++ "bundleMuons.json" else (outPath ++ "/" ++ "bundleMuons.json"))}},
        dsEvents, numTrials),
      new DatasetQuery("""d.filter(_.muons.length >= 2).flatMap({e: Event => for (i <- 0 until e.muons.length; j <- 0 until e.muons.length) yield buildDiCandidate(e.muons(i), e.muons(j))}).rdd.aggregate(emptyDiCandidate)(new Increment, new Combine)""",
        {d: Dataset[Event] => {val h = d.filter(_.muons.length >= 2)
          .flatMap({e: Event => for (i <- 0 until e.muons.length; j <- 0 until e.muons.length) yield buildDiCandidate(e.muons(i), e.muons(j))})
          .rdd.aggregate(emptyDiCandidate)(new Increment, new Combine);
          h("pt").println; 
          h.toJsonFile(if (outPath.last == '/') outPath ++ "bundleDiMuons.json" 
          else (outPath ++ "/" ++ "bundleDiMuons.json"))}},
        dsEvents, numTrials)
    )

    // start running all tests
    if (testsToRun contains 1) 
      for (i <- 0 until b1.length; x=b1(i)) 
      {
        spark.sparkContext.setJobGroup(s"B1.Q$i", x.name)
        x.run
      }
    if (testsToRun contains 2) 
      for (i <- 0 until b2.length; x=b2(i)) 
      {
        spark.sparkContext.setJobGroup(s"B2.Q$i", x.name)
        x.run
      }
    if (testsToRun contains 3) 
      for (i <- 0 until b3.length; x=b3(i)) 
      {
        spark.sparkContext.setJobGroup(s"B3.Q$i", x.name)
        x.run
      }
    if (testsToRun contains 4) 
      for (i <- 0 until b4.length; x=b4(i)) 
      {
        spark.sparkContext.setJobGroup(s"B4.Q$i", x.name)
        x.run
      }
    if (testsToRun contains 5) 
      for (i <- 0 until b5.length; x=b5(i)) 
      {
        spark.sparkContext.setJobGroup(s"B5.Q$i", x.name)
        x.run
      }
    if (testsToRun contains 6)
      for (i <- 0 until b6.length; x=b6(i)){
        spark.sparkContext.setJobGroup(s"B6.Q$i", x.name)
        x.run
      }

   spark.stop
  }
}
