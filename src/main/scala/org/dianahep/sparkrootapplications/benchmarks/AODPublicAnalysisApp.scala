package org.dianahep.sparkrootapplications.benchmarks

// defs from metrics listener
import org.dianahep.sparkrootapplications.metrics.{JobClone, StageClone, TaskClone}

// deserialize json
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}

// fs
import java.nio.file.{Paths, Path}
import java.io.{File, FileFilter}
import scala.io.Source

// breeze
import breeze.plot._

// gnuplot-based scalaplot
import org.sameersingh.scalaplot.Implicits._
import org.sameersingh.scalaplot._

class FileFilterImpl extends FileFilter {
  def accept(pathName: File): Boolean = pathName.getName.endsWith(".json")
}

object AODPublicAnalysisApp {
  def main(args: Array[String]) {
    val pathToFiles = args(0)
    val pathToPlots = "/Users/vk/software/diana-hep/intel/results/test/"

    def unwrapName(name: String) = {
      val spl = name.split("__")(0).split("_")
      (spl(0).toInt, spl(1).toInt)
    }

    // open all the files
    implicit val formats = Serialization.formats(NoTypeHints)
    val f = new File(pathToFiles)
    val tests = 
      for (x <- f.listFiles(new FileFilterImpl)) 
        yield 
          (unwrapName(x.getName), read[List[JobClone]](Source.fromFile(x.getAbsolutePath).getLines.toSeq.head))

    val flattened = tests.flatMap({x: ((Int, Int), List[JobClone]) =>
      x._2.map(((x._1._1, x._1._2), _))})
            //.groupBy(_.groupId).mapValues({vs: List[JobClone] => vs.map({v: JobClone => 
            //  (unwrapName(x.getName), v)})})

    // we now have a List[(#execs, #threads, Job)]
    type ValueType = Long
    type JobListType = List[((Int, Int), ValueType)]

    for (x <- flattened.unzip._2; stage <- x.stages.toSeq.unzip._2)
      println(s"jobGroupId=${x.groupId} numStages=${x.stages.toSeq.length} numTasks=${stage.numTasks} tasksLength=${stage.tasks.toSeq.length} stageId=${stage.id}, stageName=${stage.name}")

    //
    // 
    //
    val flattenedVaryingThreads = flattened.filter(_._1._1 == 14)
    val flattenedVaryingExecutors = flattened.filter(_._1._2 == 70)

    // group queries by group id
    // Map[JobId -> List[(#execs, #threads), Job]]
    val groupedVaryingThreads = flattenedVaryingThreads.groupBy(_._2.groupId).filter(_._1!="")
    // group queries by group id
    // Map[JobId -> List[(#execs, #threads), Job]]
    val groupedVaryingExecutors = flattenedVaryingExecutors.groupBy(_._2.groupId).filter(_._1!="")

    // job total time vs #threads*#execs keeping
    // we need List(XY(), XY(), XY())
    val jobTimesVaryingThreads = groupedVaryingThreads.toSeq.map({
      x: (String, Array[((Int, Int), JobClone)]) =>
        XY(x._2.map({p: ((Int, Int), JobClone) => 
          ((p._1._1*p._1._2).toDouble, ((p._2.endTime-p._2.startTime)/1000.0).toDouble)})
        , x._1, style=XYPlotStyle.Points)
    })
    output(PNG(pathToPlots, "jobTimesVaryingThreads"), xyChart(jobTimesVaryingThreads, 
      showLegend=true, legendPosX=LegendPosX.Right, legendPosY=LegendPosY.Top,
      title = "Job Execution Time vs Parallelism(14execs const, varying #threads)",
      x = Axis(label = "Parallelism(14execs, varying Threads)"),
      y = Axis(label = "Duration(s)")))

    //
    // Sum CPU Time over all Tasks / Sum over Duration for all Tasks
    //
    def computeTotalCPUPerStage(stage: StageClone): Double = {
      stage.tasks.toSeq.unzip._2.foldLeft[Double](0.0)(_ + _.metrics.exeCpuTime)
    }
    def computeTotalExeRunTimePerStage(stage: StageClone): Double = {
      stage.tasks.toSeq.unzip._2.foldLeft[Double](0.0)(_ + _.metrics.exeRunTime)
    }
    def computeTotalJVMGCTimePerStage(stage: StageClone): Double = {
      stage.tasks.toSeq.unzip._2.foldLeft[Double](0.0)(_ + _.metrics.jvmGCTime)
    }
    def computeTotalExeDeserTimePerStage(stage: StageClone): Double = {
      stage.tasks.toSeq.unzip._2.foldLeft[Double](0.0)(_ + _.metrics.exeDeserializeTime)
    }
    def computeTotalExeDeserCpuTimePerStage(stage: StageClone): Double = {
      stage.tasks.toSeq.unzip._2.foldLeft[Double](0.0)(_ + _.metrics.exeDeserializeCpuTime)
    }
    def computeTotalResultSerTimePerStage(stage: StageClone): Double = {
      stage.tasks.toSeq.unzip._2.foldLeft[Double](0.0)(_ + _.metrics.resultSerializationTime)
    }
    def computeTotalGettingResultTimePerStage(stage: StageClone): Double = {
      stage.tasks.toSeq.unzip._2.foldLeft[Double](0.0)(_ + _.gettingResultTime)
    }
    def computeTotalDurationPerStage(stage: StageClone): Double = {
      stage.tasks.toSeq.unzip._2.foldLeft[Double](0.0)(_ + _.duration)
    }
    def computeTotalCPUPerJob(job: JobClone): Double = {
      job.stages.toSeq.unzip._2.map(computeTotalCPUPerStage(_)).sum/1000000000
    }
    def computeTotalExeRunTimePerJob(job: JobClone): Double = {
      job.stages.toSeq.unzip._2.map(computeTotalExeRunTimePerStage(_)).sum/1000
    }
    def computeTotalJVMGCTimePerJob(job: JobClone): Double = {
      job.stages.toSeq.unzip._2.map(computeTotalJVMGCTimePerStage(_)).sum/1000
    }
    def computeTotalExeDeserTimePerJob(job: JobClone): Double = {
      job.stages.toSeq.unzip._2.map(computeTotalExeDeserTimePerStage(_)).sum/1000
    }
    def computeTotalExeDeserCpuTimePerJob(job: JobClone): Double = {
      job.stages.toSeq.unzip._2.map(computeTotalExeDeserCpuTimePerStage(_)).sum/1000000000
    }
    def computeTotalResultSerTimePerJob(job: JobClone): Double = {
      job.stages.toSeq.unzip._2.map(computeTotalResultSerTimePerStage(_)).sum/1000
    }
    def computeTotalGettingResultTimePerJob(job: JobClone): Double = {
      job.stages.toSeq.unzip._2.map(computeTotalGettingResultTimePerStage(_)).sum/1000
    }
    def computeTotalDurationPerJob(job: JobClone): Double = 
      job.stages.toSeq.unzip._2.map(computeTotalDurationPerStage(_)).sum/1000
    
    val cpuPercentageVaryingThreads = groupedVaryingThreads.toSeq.map({
      x: (String, Array[((Int, Int), JobClone)]) => 
        XY(x._2.map({p: ((Int, Int), JobClone) =>
          ((p._1._1*p._1._2).toDouble, computeTotalCPUPerJob(p._2)/computeTotalDurationPerJob(p._2))}), x._1, style=XYPlotStyle.Points)
    })
    output(PNG(pathToPlots, "cpuPercentageVaryingThreads"), 
      xyChart(cpuPercentageVaryingThreads, showLegend=true, legendPosX=LegendPosX.Right,
        legendPosY=LegendPosY.Top, title="CPU Percentage vs Parallelism (14 execs const, varying #threads)", x=Axis(label = "Parallelism(14execs, varying Threads)"),
        y = Axis(label = "Sum CPU Time per Task / Sum Duration Time per Task")))
    
    //
    // CPU Total varying number of threads
    //
    val cpuTotalVaryingThreads = groupedVaryingThreads.toSeq.map({
      x: (String, Array[((Int, Int), JobClone)]) => 
        XY(x._2.map({p: ((Int, Int), JobClone) =>
          ((p._1._1*p._1._2).toDouble, computeTotalCPUPerJob(p._2))}), x._1, style=XYPlotStyle.Points)
    })
    output(PNG(pathToPlots, "cpuTotalVaryingThreads"), 
      xyChart(cpuTotalVaryingThreads, showLegend=true, legendPosX=LegendPosX.Right,
        legendPosY=LegendPosY.Top, title="Total CPU Time vs Parallelism (14 execs const, varying #threads)", x=Axis(label = "Parallelism(14execs, varying Threads)"),
        y = Axis(label = "Sum CPU Time over all Tasks per Job")))
    
    //
    // Exe Run time varying number of threads
    //
    val exeRunTimeTotalVaryingThreads = groupedVaryingThreads.toSeq.map({
      x: (String, Array[((Int, Int), JobClone)]) => 
        XY(x._2.map({p: ((Int, Int), JobClone) =>
          ((p._1._1*p._1._2).toDouble, computeTotalExeRunTimePerJob(p._2))}), x._1, style=XYPlotStyle.Points)
    })
    output(PNG(pathToPlots, "exeRunTimeTotalVaryingThreads"), 
      xyChart(exeRunTimeTotalVaryingThreads, 
              showLegend=true, legendPosX=LegendPosX.Right,
              legendPosY=LegendPosY.Top, title="Total Executor Run Time vs Parallelism (14 execs const, varying #threads)", x=Axis(label = "Parallelism(14execs, varying Threads)"),
              y = Axis(label = "Sum ExeRunTime over all Tasks per Job")))

    //
    // JVMGC varying number of threads
    //
    val jvmGCTimeTotalVaryingThreads = groupedVaryingThreads.toSeq.map({
      x: (String, Array[((Int, Int), JobClone)]) => 
        XY(x._2.map({p: ((Int, Int), JobClone) =>
          ((p._1._1*p._1._2).toDouble, computeTotalJVMGCTimePerJob(p._2))}), x._1, style=XYPlotStyle.Points)
    })
    output(PNG(pathToPlots, "jvmGCTimeTotalVaryingThreads"), 
      xyChart(jvmGCTimeTotalVaryingThreads, 
              showLegend=true, legendPosX=LegendPosX.Right,
              legendPosY=LegendPosY.Top, title="Total JVM GC Time vs Parallelism (14 execs const, varying #threads)", x=Axis(label = "Parallelism(14execs, varying Threads)"),
              y = Axis(label = "Sum JVM GC Time over all Tasks per Job")))

    //
    // Executor Deserialization CPUT Time varying the number of threads
    //
    val exeDeserCpuTimeTotalVaryingThreads = groupedVaryingThreads.toSeq.map({
      x: (String, Array[((Int, Int), JobClone)]) => 
        XY(x._2.map({p: ((Int, Int), JobClone) =>
          ((p._1._1*p._1._2).toDouble, computeTotalExeDeserCpuTimePerJob(p._2))}), x._1, style=XYPlotStyle.Points)
    })
    output(PNG(pathToPlots, "exeDeserCpuTimeTotalVaryingThreads"), 
      xyChart(exeDeserCpuTimeTotalVaryingThreads, 
              showLegend=true, legendPosX=LegendPosX.Right,
              legendPosY=LegendPosY.Top, title="Total Executor Deser. CPU Time vs Parallelism (14 execs const, varying #threads)", x=Axis(label = "Parallelism(14execs, varying Threads)"),
              y = Axis(label = "Sum Exe Deser. CPU Time over all Tasks per Job")))

    //
    // Exe Deserialization Time ...
    //
    val exeDeserTimeTotalVaryingThreads = groupedVaryingThreads.toSeq.map({
      x: (String, Array[((Int, Int), JobClone)]) => 
        XY(x._2.map({p: ((Int, Int), JobClone) =>
          ((p._1._1*p._1._2).toDouble, computeTotalExeDeserTimePerJob(p._2))}), x._1, style=XYPlotStyle.Points)
    })
    output(PNG(pathToPlots, "exeDeserTimeTotalVaryingThreads"), 
      xyChart(exeDeserCpuTimeTotalVaryingThreads, 
              showLegend=true, legendPosX=LegendPosX.Right,
              legendPosY=LegendPosY.Top, title="Total Executor Deser. Time vs Parallelism (14 execs const, varying #threads)", x=Axis(label = "Parallelism(14execs, varying Threads)"),
              y = Axis(label = "Sum Exe Deser. Time over all Tasks per Job")))

    //
    // Total Duration varying number of threads
    //
    val durationTotalVaryingThreads = groupedVaryingThreads.toSeq.map({
      x: (String, Array[((Int, Int), JobClone)]) => 
        XY(x._2.map({p: ((Int, Int), JobClone) =>
          ((p._1._1*p._1._2).toDouble, computeTotalDurationPerJob(p._2))}), x._1, style=XYPlotStyle.Points)
    })
    output(PNG(pathToPlots, "durationTotalVaryingThreads"), 
      xyChart(durationTotalVaryingThreads, showLegend=true, legendPosX=LegendPosX.Right,
        legendPosY=LegendPosY.Top, title="Total Duration vs Parallelism (14 execs const, varying #threads)", x=Axis(label = "Parallelism(14execs, varying Threads)"),
        y = Axis(label = "Sum Duration over all Tasks per Job")))

    
    // job total time vs #
    val jobTimesVaryingExecutors = groupedVaryingExecutors.toSeq.map({
      x: (String, Array[((Int, Int), JobClone)]) =>
        XY(x._2.map({p: ((Int, Int), JobClone) => 
          ((p._1._1*p._1._2).toDouble, ((p._2.endTime-p._2.startTime)/1000.0).toDouble)})
        , x._1, style=XYPlotStyle.Points)
    })
    output(PNG(pathToPlots, "jobTimesVaryingExecutors"), xyChart(jobTimesVaryingExecutors, 
      showLegend=true, legendPosX=LegendPosX.Right, legendPosY=LegendPosY.Top,
      title = "Job Execution Time vs Parallelism(#execs floats, 70threads const)",
      x = Axis(label = "Parallelism(#execs floats, 70threads const)"),
      y = Axis(label = "Duration (s)")))

    val cpuPercentageVaryingExecutors = groupedVaryingExecutors.toSeq.map({
      x: (String, Array[((Int, Int), JobClone)]) => 
        XY(x._2.map({p: ((Int, Int), JobClone) =>
          ((p._1._1*p._1._2).toDouble, computeTotalCPUPerJob(p._2)/computeTotalDurationPerJob(p._2))}), x._1, style=XYPlotStyle.Points)
    })
    output(PNG(pathToPlots, "cpuPercentageVaryingExecutors"), 
      xyChart(cpuPercentageVaryingExecutors, showLegend=true, legendPosX=LegendPosX.Right,
        legendPosY=LegendPosY.Top, title="CPU Percentage vs Parallelism(#execs floats, 70threads const)", x=Axis(label = "Parallelism(#execs floats, 70threads const)"),
        y = Axis(label = "Sum CPU Time per Task / Sum Duration Time per Task")))
    val cpuTotalVaryingExecutors = groupedVaryingExecutors.toSeq.map({
      x: (String, Array[((Int, Int), JobClone)]) => 
        XY(x._2.map({p: ((Int, Int), JobClone) =>
          ((p._1._1*p._1._2).toDouble, computeTotalCPUPerJob(p._2))}), x._1, style=XYPlotStyle.Points)
    })
    output(PNG(pathToPlots, "cpuTotalVaryingExecutors"), 
      xyChart(cpuTotalVaryingExecutors, showLegend=true, legendPosX=LegendPosX.Right,
        legendPosY=LegendPosY.Top, title="Total CPU Time vs Parallelism (#execs floats, 70threads const)", x=Axis(label = "Parallelism(#execs, 70threads"),
        y = Axis(label = "Sum CPU Time over all Tasks per Job")))

    val durationTotalVaryingExecutors = groupedVaryingExecutors.toSeq.map({
      x: (String, Array[((Int, Int), JobClone)]) => 
        XY(x._2.map({p: ((Int, Int), JobClone) =>
          ((p._1._1*p._1._2).toDouble, computeTotalDurationPerJob(p._2))}), x._1, style=XYPlotStyle.Points)
    })
    output(PNG(pathToPlots, "durationTotalVaryingExecutors"), 
      xyChart(durationTotalVaryingExecutors, showLegend=true, legendPosX=LegendPosX.Right,
        legendPosY=LegendPosY.Top, title="Total Duration vs Parallelism (#execs floats, 70threads const)", x=Axis(label = "Parallelism(#execs, 70threads)"),
        y = Axis(label = "Sum Duration over all Tasks per Job")))

    //
    // time per stage for varying threads case
    //
    for (x <- groupedVaryingThreads.toSeq; groupId=x._1; queries=x._2) {
      val stages = queries.flatMap({
        // sort so that stage ids are in increasing order!
        q: ((Int, Int), JobClone) => {
          val sorts = q._2.stages.toSeq.unzip._2.sortWith(_.id < _.id)
          // set the ids of each stage as the order number in the sequence
          sorts.map({stage: StageClone => {stage.id=sorts.indexWhere(_.id==stage.id); stage}})
            .map({stage: StageClone => ((q._1._1, q._1._2), stage)})
        }
      })
      val groupedStages = stages.groupBy(_._2.id).map({
        g: (Int, Array[((Int, Int), StageClone)]) =>
          XY(g._2.map({p: ((Int, Int), StageClone) => ((p._1._1*p._1._2).toDouble,
            ((p._2.completionTime-p._2.submissionTime)/1000.0).toDouble)}), s"ID${g._1}",
            style=XYPlotStyle.Points)
      })
      output(PNG(pathToPlots, s"timePerStageVaryingThreads__$groupId"), xyChart(groupedStages,
        showLegend=true, legendPosX=LegendPosX.Right, legendPosY=LegendPosY.Top,
        title=s"$groupId: Execution Time per Stage vs Parallelism(14execs const, varying #threads)",
        x = Axis(label = "Parallelism(14execs, varying Threads)"),
        y = Axis(label = "Duration(s)", log = true)))
    }
    
    // 
    // time per stage for varying executors case
    //
    for (x <- groupedVaryingExecutors.toSeq; groupId=x._1; queries=x._2) {
      val stages = queries.flatMap({
        // sort so that stage ids are in increasing order!
        q: ((Int, Int), JobClone) => {
          val sorts = q._2.stages.toSeq.unzip._2.sortWith(_.id < _.id)
          // set the ids of each stage as the order number in the sequence
          sorts.map({stage: StageClone => {stage.id=sorts.indexWhere(_.id==stage.id); stage}})
            .map({stage: StageClone => ((q._1._1, q._1._2), stage)})
        }
      })
      val groupedStages = stages.groupBy(_._2.id).map({
        g: (Int, Array[((Int, Int), StageClone)]) =>
          XY(g._2.map({p: ((Int, Int), StageClone) => ((p._1._1*p._1._2).toDouble,
            ((p._2.completionTime-p._2.submissionTime)/1000.0).toDouble)}), s"ID${g._1}",
            style=XYPlotStyle.Points)
      })
      output(PNG(pathToPlots, s"timePerStageVaryingExecutors__$groupId"), xyChart(groupedStages,
        showLegend=true, legendPosX=LegendPosX.Right, legendPosY=LegendPosY.Top,
        title=s"$groupId: Execution Time per Stage vs Parallelism(#execs floats, 70threads const)",
        x = Axis(label = "Parallelism(#execs floats, 70threads const)"),
        y = Axis(label = "Duration(s)", log = true)))

      //
      // for each stage, plot the distribution of timing vs executorId
      //
      var counter = 0;
      for (stage <- stages) {
        output(PNG(pathToPlots, s"timeDistributionVsExecutorId__${stage._1._1}__${stage._1._2}__${groupId}__${stage._2.id}__${counter}"), 
          xyChart(Seq(
            XY(stage._2.tasks.toSeq.unzip._2.map({t: TaskClone => (t.executorId.toDouble, (t.duration).toDouble/1000.0)}), "Task.duration", style=XYPlotStyle.Points), 
            XY(stage._2.tasks.toSeq.unzip._2.map({t: TaskClone => (t.executorId.toDouble, ((t.finishTime-t.launchTime)/1000.0).toDouble)}), "finish-launch",style=XYPlotStyle.Points), 
            XY(stage._2.tasks.toSeq.unzip._2.map({t: TaskClone => (t.executorId.toDouble, ((t.gettingResultTime)).toDouble/1000.0)}), "gettingResultTime",style=XYPlotStyle.Points),
            XY(stage._2.tasks.toSeq.unzip._2.map({t: TaskClone => (t.executorId.toDouble, ((t.metrics.exeRunTime)).toDouble/1000.0)}), "exeRunTime",style=XYPlotStyle.Points),
            XY(stage._2.tasks.toSeq.unzip._2.map({t: TaskClone => (t.executorId.toDouble, ((t.metrics.exeCpuTime)/1000000000.0).toDouble)}), "exeCPUTime", style=XYPlotStyle.Points),
            XY(stage._2.tasks.toSeq.unzip._2.map({t: TaskClone => (t.executorId.toDouble, ((t.metrics.exeDeserializeCpuTime)/1000000000.0).toDouble)}), "exeDeserCPUTime",style=XYPlotStyle.Points),
            XY(stage._2.tasks.toSeq.unzip._2.map({t: TaskClone => (t.executorId.toDouble, ((t.metrics.exeDeserializeTime)/1000000000.0).toDouble)}), "exeDeserTime",style=XYPlotStyle.Points),
            XY(stage._2.tasks.toSeq.unzip._2.map({t: TaskClone => (t.executorId.toDouble, ((t.metrics.jvmGCTime)).toDouble/1000.0)}), "jvmGCTime",style=XYPlotStyle.Points)
          ),
            showLegend=true, legendPosX=LegendPosX.Right, legendPosY=LegendPosY.Top,
            title=s"$groupId: Time vs ExecutorId(${stage._1._1}execs, ${stage._1._2}threads)", x = Axis("ExecutorId"), y = Axis(label = "Duration(s)")))
        counter += 1
      }

      //
      // vs task Id
      //
      counter = 0;
      for (stage <- stages) {
        output(PNG(pathToPlots, s"timeDistributionVsTaskId__${stage._1._1}__${stage._1._2}__${groupId}__${stage._2.id}__${counter}"), 
          xyChart(Seq(
            XY(stage._2.tasks.toSeq.unzip._2.map({t: TaskClone => (t.id.toDouble, (t.duration).toDouble/1000.0)}), "Task.duration", style=XYPlotStyle.Points), 
            XY(stage._2.tasks.toSeq.unzip._2.map({t: TaskClone => (t.id.toDouble, ((t.finishTime-t.launchTime)).toDouble/1000.0)}), "finish-launch",style=XYPlotStyle.Points), 
            XY(stage._2.tasks.toSeq.unzip._2.map({t: TaskClone => (t.id.toDouble, ((t.gettingResultTime)).toDouble/1000.0)}), "gettingResultTime",style=XYPlotStyle.Points),
            XY(stage._2.tasks.toSeq.unzip._2.map({t: TaskClone => (t.id.toDouble, ((t.metrics.exeRunTime)).toDouble/1000.0)}), "exeRunTime",style=XYPlotStyle.Points),
            XY(stage._2.tasks.toSeq.unzip._2.map({t: TaskClone => (t.id.toDouble, ((t.metrics.exeCpuTime)/1000000000.0).toDouble)}), "exeCPUTime", style=XYPlotStyle.Points),
            XY(stage._2.tasks.toSeq.unzip._2.map({t: TaskClone => (t.id.toDouble, ((t.metrics.exeDeserializeCpuTime)/1000000000.0).toDouble)}), "exeDeserCPUTime",style=XYPlotStyle.Points),
            XY(stage._2.tasks.toSeq.unzip._2.map({t: TaskClone => (t.id.toDouble, ((t.metrics.exeDeserializeTime)/1000000000.0).toDouble)}), "exeDeserTime",style=XYPlotStyle.Points),
            XY(stage._2.tasks.toSeq.unzip._2.map({t: TaskClone => (t.id.toDouble, ((t.metrics.jvmGCTime)).toDouble/1000.0)}), "jvmGCTime",style=XYPlotStyle.Points)
          ),
            showLegend=true, legendPosX=LegendPosX.Right, legendPosY=LegendPosY.Top,
            title=s"$groupId: Time vs TaskId(${stage._1._1}execs, ${stage._1._2}threads)", x = Axis("TaskId"), y = Axis(label = "Duration(s)")))
        counter += 1
      }
    }
  }
}
