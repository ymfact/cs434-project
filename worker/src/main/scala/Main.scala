
import java.io.File
import java.util.concurrent.Executors

import Worker.Context

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

object Main {
  implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor)

  def main(args: Array[String]): Unit = {
    System.setProperty("out-dir", args.last) // for log
    val ctx = new Context(
      masterDest = args.head,
      in = args.slice(2, args.length - 2).map(new File(_)),
      out = new File(args.last)
    )
    val worker = new Worker(ctx)
    worker.blockUntilShutdown()
  }
}
