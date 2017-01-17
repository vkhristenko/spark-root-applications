# spark-root-examples
Examples for spark-root

## Instructions on how to Run
```

Higgs Example, given you have an input file in $DATASOURCEDIR

./spark-submit --class "org.dianahep.sparkrootexamples.apps.HiggsExampleApp" --master local[4] --packages org.diana-hep:spark-root_2.11:0.1.6,org.diana-hep:histogrammar-sparksql_2.11:1.0.3,org.diana-hep:histogrammar-bokeh_2.11:1.0.3 /Users/vk/software/diana-hep/spark-root-examples/target/scala-2.11/spark-root-examples_2.11-0.0.1.jar $DATASOURCEDIR/ntuple_drellyan_test.root
```
