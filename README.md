# Spark ROOT Applications
Collection of various examples/utilities for Spark ROOT.

## Metrics Listener
Simple Metrics Listener that extends SparkListener. All the jobs are collected in a buffer and dumped into json at the application's end.
Directory/Filename of the metrics output are configurable (see below the conf options)

```
source for MetricsListener is located here: src/main/scala/org/dianahep/sparkrootapplications/metrics
sources for BenchMarksApp are here: src/main/scala/org/dianahep/sparkrootapplications/benchmarks

./spark-submit  --packages org.diana-hep:spark-root_2.11:0.1.7,org.diana-hep:histogrammar-sparksql_2.11:1.0.3,org.json4s:json4s-native_2.11:3.2.11  --class "org.dianahep.sparkrootapplications.benchmarks.AODPublicBenchmarkApp"  --conf spark.extraListeners="org.dianahep.sparkrootapplications.metrics.MetricsListener" --conf spark.executorEnv.pathToMetrics=/Users/vk/software/diana-hep/intel/metrics --conf spark.executorEnv.metricsFileName=test.json /Users/vk/software/diana-hep/spark-root-applications/target/scala-2.11/spark-root-applications_2.11-0.0.1.jar  file:/Users/vk/software/diana-hep/test_data/0029E804-C77C-E011-BA94-00215E22239A.root  /Users/vk/software/diana-hep/test_data/benchmarks/test 1 5
```

## Example Applications
- ReductionExample App
- AODPublicExample App
- Bacon/Higgs Example Apps
