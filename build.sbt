name := "spark-root-examples"

organization := "org.diana-hep"

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

version := "0.0.1"

isSnapshot := true

scalaVersion := "2.11.8"

spIgnoreProvided := true
sparkVersion := "2.0.0"
sparkComponents := Seq("sql")

resolvers += Resolver.mavenLocal
libraryDependencies += "org.diana-hep" % "spark-root_2.11" % "0.1.4"
libraryDependencies += "org.diana-hep" % "histogrammar-sparksql_2.11" % "1.0.3"
libraryDependencies += "org.diana-hep" % "histogrammar-bokeh_2.11" % "1.0.3"
