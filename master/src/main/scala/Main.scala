import Master.{Context, Parser}
import org.backuity.clist.Cli

object Main {
  def main(args: Array[String]): Unit = {
    Cli.parse(args).withCommand(new Parser) { case parser =>
      val ctx = new Context(parser.dir, parser.workerCount)
      new Master(ctx)
    }
  }
}
