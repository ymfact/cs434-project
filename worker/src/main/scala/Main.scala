
import Worker.{Context, Parser}
import org.backuity.clist.Cli

object Main {
  def main(args: Array[String]): Unit = {
    Cli.parse(args).withCommand(new Parser) { case parser =>
      val ctx = new Context(parser.dir, parser.workerIndex)
      new Worker(ctx)
    }
  }
}
