
import java.nio.file.Paths

import Worker.{Context, Parser}
import org.backuity.clist.Cli

import scala.concurrent.ExecutionContext

object Main {
  def main(args: Array[String]): Unit = {
    Cli.parse(args).withCommand(new Parser) { parser =>
      System.setProperty("worker-index-for-log", parser.workerIndex.toString)
      val ctx = new Context(
        rootDir = parser.dir,
        workerCount = parser.workerCount,
        workerIndex = parser.workerIndex,
        partitionCount = parser.partitionCount,
        partitionSize = parser.partitionSize,
        sampleCount = parser.sampleCount,
        isBinary = parser.isBinary)
      val worker = new Worker(ExecutionContext.global, ctx)
      worker.start()
      worker.blockUntilShutdown()
    }
  }
}
