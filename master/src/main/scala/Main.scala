object Main {
  def main(args: Array[String]): Unit = {
    val worker_count = if(args.nonEmpty) args(0).toInt else 1
    new Master(worker_count)
  }
}
