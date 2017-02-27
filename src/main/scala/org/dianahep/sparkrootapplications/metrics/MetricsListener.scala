package org.dianahep.sparkrootapplications.metrics

// import spark deps
import org.apache.spark.scheduler.{SparkListenerStageCompleted, SparkListener, SparkListenerJobStart, SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerJobEnd, SparkListenerStageSubmitted, SparkListenerTaskStart, SparkListenerTaskGettingResult, SparkListenerTaskEnd, SparkListenerExecutorAdded, SparkListenerExecutorMetricsUpdate, SparkListenerExecutorRemoved}
import org.apache.spark.SparkConf

import org.apache.log4j.{Level, LogManager, PropertyConfigurator}
import scala.collection.mutable.ListBuffer
import java.io._

// for json4s http://json4s.org/
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}

// nio java
import java.nio.file.{Paths, Path}

//
// Custom Metrics Listener. To use with spark:
// --conf spark.executorEnv.pathToMetrics=somePath 
// --confspark.executorEnv.metricsFileName=someFileName
//
// predefined configs:
// 1. spark.executorEnv.pathToMetrics - path to the folder where to create json metrics
//   defaults to /tmp
// 2. spark.executorEnv.metricsFileName - filename for metrics, default `metrics.json`
//   defaults to metrics.json
// 3. SparkListenerJobStart should have properties:
//   - spark.job.description
//   - spark.job.id
//   - if those are not provided - defaults to empty strings
//
class MetricsListener(conf: SparkConf) extends SparkListener {
  // logger
  lazy val logger = LogManager.getLogger("org.dianahep.sparkrootapplications.metrics")
  val pathToMetrics = conf.get("spark.executorEnv.pathToMetrics", "/tmp")
  val metricsFileName = conf.get("spark.executorEnv.metricsFileName", "metrics.json")
  val jobDescString = "spark.job.description";
  val jobIdString = "spark.jobGroup.id";

  case class Job(var id: Int, var groupId: String, 
                 var desc: String, var startTime: Long, var endTime: Long);
  var currentJob: Job = null
  var jobs: ListBuffer[Job] = ListBuffer.empty[Job];

  // application start/end
  override def onApplicationEnd(appEnd: SparkListenerApplicationEnd) {
    logger.info(s"Application ended $appEnd with #jobs=${jobs.length}")
    for (x <- jobs) logger.info(s"Job $x")

    // from http://json4s.org/ 
    implicit val formats = Serialization.formats(NoTypeHints)
    val json = write(jobs)
    val fullPath = Paths.get(pathToMetrics, metricsFileName).toString
    val writter = new FileWriter(fullPath, false)
    writter.write(s"${json}\n")
    writter.close
  }

  override def onApplicationStart(appStart: SparkListenerApplicationStart) {
    logger.info(s"Application started $appStart.")
  }

  // job start/end
  override def onJobStart(jobStart: SparkListenerJobStart) {
    logger.info(s"Job started with ${jobStart.stageInfos.size} stages: $jobStart properties: ${jobStart.properties}")

    // reset current job
    currentJob = jobStart match {
      case SparkListenerJobStart(jobId, time, stageInfos, props) => 
        Job(jobId, props.getProperty(jobIdString, ""), 
          props.getProperty(jobDescString,""), time, 0
        )
      case _ => null
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    currentJob.endTime = jobEnd.time
    jobs += currentJob
  }

  // stage submitted/completed
  /*
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    logger.info(s"Stage ${stageCompleted.stageInfo.stageId} completed with ${stageCompleted.stageInfo.numTasks} tasks.")
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) = {
    logger.info(s"Stage submitted $stageSubmitted")
  }*/

  // task start/end/gettingResult
  /*
  override def onTaskStart(taskStart: SparkListenerTaskStart) = {
    logger.info(s"Task started $taskStart")
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) = {
    logger.info(s"Task Getting Result $taskGettingResult")
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = {
    logger.info(s"Task Edn $taskEnd")
  }*/
}
