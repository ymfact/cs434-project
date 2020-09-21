
import Worker.{Context, Parser}
import org.backuity.clist.Cli

object Main {
  def main(args: Array[String]): Unit = {
    Cli.parse(args).withCommand(new Parser) { parser =>
      val ctx = new Context(
        rootDir = parser.dir,
        workerCount = parser.workerCount,
        workerIndex = parser.workerIndex,
        partitionCount = parser.partitionCount,
        partitionSize = parser.partitionSize,
        isBinary = parser.isBinary)
      new Worker(ctx)
    }
  }
}
