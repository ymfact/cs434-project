import java.io.File

import Master.{Config, Context}

object Main {
  def main(args: Array[String]): Unit = {
    val ctx = new Context(
      workerCount = args.head.toInt
    )
    new Master(ctx)
  }
}