object Main {
  def main(args: Array[String]): Unit = {
    val worker_index = if(args.nonEmpty) args(0).toInt else 0
    new Worker(worker_index)
  }
}
