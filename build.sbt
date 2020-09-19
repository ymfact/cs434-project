name := "project434"
version in ThisBuild := "0.1"
scalaVersion in ThisBuild := "2.13.3"

lazy val root = project
  .in(file("."))
  .disablePlugins(AssemblyPlugin)
  .aggregate(common, master, worker)
  .dependsOn(master, worker)
  .settings(
    logSettings,
    libraryDependencies ++= commonDependencies
  )

lazy val common = project
  .disablePlugins(AssemblyPlugin)
  .settings(
    name := "common",
    logSettings,
    libraryDependencies ++= commonDependencies,
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value / "scalapb"
    ),
  )

lazy val master = project
  .dependsOn(common)
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "master",
    assemblySettings,
    logSettings,
    libraryDependencies ++= commonDependencies
  )

lazy val worker = project
  .dependsOn(common)
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "worker",
    assemblySettings,
    logSettings,
    libraryDependencies ++= commonDependencies ++ Seq(
      "com.lihaoyi" %% "cask" % "0.7.5",
    )
  )

lazy val commonDependencies = Seq(
  "org.backuity.clist" %% "clist-core" % "3.5.1",
  "org.backuity.clist" %% "clist-macros" % "3.5.1" % "provided",
  "org.scalaj" %% "scalaj-http" % "2.4.2",
  "org.scala-lang.modules" %% "scala-parallel-collections" % "0.2.0",
  "io.undertow" % "undertow-core" % "2.2.0.Final",

  "org.scalatest" %% "scalatest" % "3.2.2" % "test",
  "org.scalatestplus" %% "junit-4-13" % "3.2.2.0" % "test",
  "junit" % "junit" % "4.13" % "test",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.13.3",
  "org.apache.logging.log4j" % "log4j-core" % "2.13.3" % Runtime
)

lazy val assemblySettings = Seq(
  test in assembly := {},
  assemblyJarName in assembly := s"project-${name.value}-${version.value}-assembly.jar",
)

lazy val logSettings = Seq(
  javaOptions += "-Dlog4j.configurationFile=src/main/resources/log4j2.xml",
)