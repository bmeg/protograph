organization  := "io.bmeg"
name := "protograph"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.8"

conflictManager := ConflictManager.strict.copy(organization = "com.esotericsoftware.*")

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.module" %% "jackson-module-scala"     % "2.8.4",
  "net.jcazevedo"                %% "moultingyaml"             % "0.3.0",
  "com.trueaccord.scalapb"       %% "scalapb-json4s"           % "0.1.6",
  "org.scalactic"                %% "scalactic"                % "3.0.0",
  "org.scalatest"                %% "scalatest"                % "3.0.0" % "test",
  "org.scala-lang"               %  "scala-compiler"           % "2.11.8",
  "com.google.protobuf"          %  "protobuf-java"            % "3.1.0",
  "com.google.protobuf"          %  "protobuf-java-util"       % "3.1.0",
  "com.google.protobuf"          %  "protoc"                   % "3.1.0"
).map(_.exclude("org.slf4j", "slf4j-log4j12"))

resolvers += "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository"
resolvers ++= Seq(
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases"
)

PB.protoSources in Compile := Seq(new java.io.File("src/main/proto"))
PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

libraryDependencies += "com.trueaccord.scalapb" %% "scalapb-runtime" % com.trueaccord.scalapb.compiler.Version.scalapbVersion % "protobuf"

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "content/repositories/releases")
}

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
