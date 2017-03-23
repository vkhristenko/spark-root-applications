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
  case class TMetrics(var diskBytesSpilled: Long, var exeDeserializeTime: Long,
    var exeDeserializeCpuTime: Long,
    var exeRunTime: Long, var exeCpuTime: Long,
    var bytesRead: Long, var recordsRead: Long, var jvmGCTime: Long,
    var memoryBytesSpilled: Long, var bytesWritten: Long, var recordsWritten: Long,
    var resultSerializationTime: Long, var resultSize: Long);
  case class Task(var id: Long, var host: String, var executorId: String,
                  var duration: Long, var taskType: String,
                  var launchTime: Long, var finishTime: Long,
                  var gettingResultTime: Long, metrics: TMetrics);
  case class Stage(var id: Int, var name: String, var numTasks: Int, 
                   var submissionTime: Long, var completionTime: Long,
                   val tasks: scala.collection.mutable.Map[String, Task]);
  case class Job(var id: Int, var groupId: String, 
                 var desc: String, var startTime: Long, var endTime: Long,
                 val stages: scala.collection.mutable.Map[String, Stage]);
  case class TaskClone(var id: Long, var host: String, var executorId: String,
                  var duration: Long, var taskType: String,
                  var launchTime: Long, var finishTime: Long,
                  var gettingResultTime: Long, metrics: TMetrics);
  case class StageClone(var id: Int, var name: String, var numTasks: Int, 
                   var submissionTime: Long, var completionTime: Long,
                   val tasks: Map[String, TaskClone]);
  case class JobClone(var id: Int, var groupId: String, 
                 var desc: String, var startTime: Long, var endTime: Long,
                 val stages: Map[String, StageClone]);
class MetricsListener(conf: SparkConf) extends SparkListener {
  // logger
  lazy val logger = LogManager.getLogger("org.dianahep.sparkrootapplications.metrics")
  val pathToMetrics = conf.get("spark.executorEnv.pathToMetrics", "/tmp")
  val metricsFileName = conf.get("spark.executorEnv.metricsFileName", "metrics.json")
  val jobDescString = "spark.job.description";
  val jobIdString = "spark.jobGroup.id";
  
  // basic defs
  var currentJob: Job = null
  var jobs: ListBuffer[Job] = ListBuffer.empty[Job];

  // application start/end
  override def onApplicationEnd(appEnd: SparkListenerApplicationEnd) {
    logger.info(s"Application ended $appEnd with #jobs=${jobs.length}")

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
    // reset current job
    currentJob = jobStart match {
      case SparkListenerJobStart(jobId, time, stageInfos, props) => 
        Job(jobId, props.getProperty(jobIdString, ""), 
          props.getProperty(jobDescString,""), time, 0,
          scala.collection.mutable.Map.empty[String, Stage]
        )
      case _ => null
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    currentJob.endTime = jobEnd.time
    jobs += currentJob
  }

  // stage submitted/completed
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = 
  stageCompleted match {
    case SparkListenerStageCompleted(stageInfo) => {
      stageInfo.completionTime match {
        case Some(value) => currentJob.stages(stageInfo.stageId.toString).completionTime=value
        case None => -1
      }
      stageInfo.submissionTime match {
        case Some(value) => currentJob.stages(stageInfo.stageId.toString).submissionTime=value
        case None => -1
      }
    }
    case _ => None
  }

  // stage was submitted
  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) = 
  stageSubmitted match {
    case SparkListenerStageSubmitted(stageInfo, props) =>
      currentJob.stages += (stageInfo.stageId.toString -> 
        Stage(stageInfo.stageId, stageInfo.name, stageInfo.numTasks,
          stageInfo.submissionTime match {
            case Some(value) => value
            case None => -1
          }, 
          stageInfo.completionTime match {
            case Some(value) => value
            case None => -1
          }, 
          scala.collection.mutable.Map.empty[String, Task])
      )
    case _ => None
  }

  // starting a task
  override def onTaskStart(taskStart: SparkListenerTaskStart) = 
  taskStart match {
    case SparkListenerTaskStart(stageId, attemptId, taskInfo) =>
      currentJob.stages(stageId.toString).tasks += (taskInfo.taskId.toString ->
        Task(taskInfo.taskId, taskInfo.host, taskInfo.executorId, 
          -1, "", taskInfo.launchTime, -1, -1,
          TMetrics(-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1)))
    case _ => None
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) = {
    logger.info(s"Task Getting Result $taskGettingResult")
  }

  // finishing a task
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = 
  taskEnd match {
    case SparkListenerTaskEnd(stageId, attemptId, taskType, reason, 
                              taskInfo, taskMetrics) => {
      currentJob.stages(stageId.toString).tasks(taskInfo.taskId.toString).duration = taskInfo.duration
      currentJob.stages(stageId.toString).tasks(taskInfo.taskId.toString).taskType = taskType
      currentJob.stages(stageId.toString).tasks(taskInfo.taskId.toString).finishTime = taskInfo.finishTime
      currentJob.stages(stageId.toString).tasks(taskInfo.taskId.toString).gettingResultTime = taskInfo.gettingResultTime

      // get all the metrics
      currentJob.stages(stageId.toString).tasks(taskInfo.taskId.toString).metrics.diskBytesSpilled = taskMetrics.diskBytesSpilled
      currentJob.stages(stageId.toString).tasks(taskInfo.taskId.toString).metrics.exeDeserializeTime = taskMetrics.executorDeserializeTime
      currentJob.stages(stageId.toString).tasks(taskInfo.taskId.toString).metrics.exeRunTime = taskMetrics.executorRunTime
      currentJob.stages(stageId.toString).tasks(taskInfo.taskId.toString).metrics.exeCpuTime = taskMetrics.executorCpuTime
      currentJob.stages(stageId.toString).tasks(taskInfo.taskId.toString).metrics.exeDeserializeCpuTime = taskMetrics.executorDeserializeCpuTime
      currentJob.stages(stageId.toString).tasks(taskInfo.taskId.toString).metrics.jvmGCTime = taskMetrics.jvmGCTime
      currentJob.stages(stageId.toString).tasks(taskInfo.taskId.toString).metrics.resultSerializationTime = taskMetrics.resultSerializationTime

      // input metrics - for now not working properly!
      currentJob.stages(stageId.toString).tasks(taskInfo.taskId.toString).metrics.bytesRead = taskMetrics.inputMetrics.bytesRead
      currentJob.stages(stageId.toString).tasks(taskInfo.taskId.toString).metrics.recordsRead = taskMetrics.inputMetrics.recordsRead
    }
    case _ => None
  }
}
