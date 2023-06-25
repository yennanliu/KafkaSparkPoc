package dev

// https://docs.scala-lang.org/tour/pattern-matching.html

object app2 extends App {

  sealed trait Skill

  case class BackendDevelopment(name: String) extends Skill

  case class DataEngineering(name: String) extends Skill

  case class SystemArchitecture(name: String) extends Skill

  def run(name: Skill): String = name match {
    case BackendDevelopment(name) => "build backend services with JVM, Python"
    case DataEngineering(name) => "big data, streaming, data platform development"
    case SystemArchitecture(name) => "system design, product ownership"
    case _ => "other awesome works"
  }

  val x1 = BackendDevelopment("")
  val res1 = run(x1)
  println(res1)

}
