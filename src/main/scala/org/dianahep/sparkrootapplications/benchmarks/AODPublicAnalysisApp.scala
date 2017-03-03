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

    // from now on, we got:
    // Map[Job Id -> List[(#execs, #threads), Job]]
    val flattenedVaryingThreads = flattened.filter(_._1._1 == 14)
    val flattenedVaryingExecutors = flattened.filter(_._1._2 == 70)

    // group queries by group id
    val groupedVaryingThreads = flattenedVaryingThreads.groupBy(_._2.groupId).filter(_._1!="")
    // group queries by group id
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
      // for each stage, plot the distribution of timing vs executorId/taskId
      //
      var counter = 0;
      for (stage <- stages) {
        output(PNG(pathToPlots, s"timeDistributionVsExecutorId__${stage._1._1}__${stage._1._2}__${groupId}__${stage._2.id}__${counter}"), 
          xyChart(XY(stage._2.tasks.toSeq.unzip._2.map({t: TaskClone => (t.executorId.toDouble, (t.duration/1000.0).toDouble)}), style=XYPlotStyle.Points),
            title=s"$groupId: Time vs ExecutorId(${stage._1._1}execs, ${stage._1._2}threads)", x = Axis("ExecutorId"), y = Axis(label = "Duration(s)")))
        counter += 1
      }
      counter = 0;
      for (stage <- stages) {
        output(PNG(pathToPlots, s"timeDistributionVsTaskId__${stage._1._1}__${stage._1._2}__${groupId}__${stage._2.id}__${counter}"), 
          xyChart(XY(stage._2.tasks.toSeq.unzip._2.map({t: TaskClone => (t.id.toDouble, (t.duration/1000.0).toDouble)}), style=XYPlotStyle.Points),
            title=s"$groupId: Time vs TaskId(${stage._1._1}execs, ${stage._1._2}threads)", x = Axis("TaskId"), y = Axis(label = "Duration(s)")))
        counter += 1
      }
      
    }
  }
}
