import Master.{Context, WorkerListener}

object Main {
  def main(args: Array[String]): Unit = {
    val workerCount = args.head.toInt
    val workerListener = WorkerListener.listenAndGetWorkerDests(workerCount){ workerDests =>
      val ctx = new Context(
        workerDests = workerDests
      )
      new Master(ctx)
    }
    println(workerListener.thisDest)
    workerListener.blockUntilShutdown()
  }
}