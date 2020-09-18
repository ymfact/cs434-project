import Master.{Context, Parser}
import org.backuity.clist.Cli

object Main {
  def main(args: Array[String]): Unit = {
    Cli.parse(args).withCommand(new Parser) { parser =>
      val ctx = new Context(
        dir = parser.dir,
        workerCount = parser.workerCount,
        partitionCount = parser.partitionCount,
        partitionSize = parser.partitionSize)
      new Master(ctx)
    }
  }
}
