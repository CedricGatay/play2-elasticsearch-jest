import play.Project._

name         := "elasticsearch-java-sample"

version      := "1.0-SNAPSHOT"

play.Project.playJavaSettings

libraryDependencies += "io.searchbox" % "jest" % "0.1.0"

lazy val esModule = RootProject(file("../../module"))

lazy val root = project.in(file(".")).dependsOn(esModule).aggregate(esModule)