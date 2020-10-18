import Master.{Context, WorkerListener}

import scala.jdk.CollectionConverters.CollectionHasAsScala

object Main {
  def main(args: Array[String]): Unit = {
    val workerCount = args.head.toInt
    val workerListener = new WorkerListener(workerCount)
    println(workerListener.thisDest)
    workerListener.blockUntilShutdown()
    val workerDests = workerListener.workerDests.asScala.toSeq
    println(workerDests.mkString(" "))
    val ctx = new Context(workerDests = workerDests)
    new Master(ctx)
  }
}