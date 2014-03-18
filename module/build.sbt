import play.Project._
import scala.Some
import xerial.sbt.Sonatype.SonatypeKeys._
import xerial.sbt.Sonatype._

name := "play2-elasticsearch-jest"

version := "0.1.0"

libraryDependencies ++= Seq(
  javaCore,
  // Add your project dependencies here
  "org.elasticsearch" % "elasticsearch" % "1.0.1",
  "io.searchbox" % "jest" % "0.1.0",
  "org.apache.commons" % "commons-lang3" % "3.1"
)

play.Project.playJavaSettings

sonatypeSettings

organization := "com.code-troopers.play"

licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

homepage := Some(url("https://github.com/CedricGatay/play2-elasticsearch-jest"))

pomExtra := (
  <scm>
    <url>git@github.com:CedricGatay/play2-elasticsearch-jest.git</url>
    <connection>scm:git:git@github.com:CedricGatay/play2-elasticsearch-jest.git</connection>
  </scm>
    <developers>
      <developer>
        <id>nboire</id>
        <name>Nicolas Boire</name>
      </developer>
      <developer>
        <id>mguillermin</id>
        <name>Matthieu Guillermin</name>
        <url>http://matthieuguillermin.fr</url>
      </developer>
      <developer>
        <id>cgatay</id>
        <name>Cedric Gatay</name>
        <url>http://www.code-troopers.com</url>
      </developer>
    </developers>)

