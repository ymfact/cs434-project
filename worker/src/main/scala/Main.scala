
import java.io.File

import Worker.Context

object Main {
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
