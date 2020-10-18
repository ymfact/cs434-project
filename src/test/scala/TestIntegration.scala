import java.nio.file.{Path, Paths}
import java.util.concurrent.Executors

import Master.{Context, WorkerListener}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala

@RunWith(classOf[JUnitRunner])
class TestIntegration extends AnyFunSuite {
  implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor)

  trait Launch {
    private val PATH: Path = Paths.get("D:/works/csed434/project_temp")
    private val WORKER_COUNT = 2

    private val workerListener = new WorkerListener(WORKER_COUNT)

    val masterFuture: Future[Unit] = Future {
      workerListener.blockUntilShutdown()
      val ctx = new Context(workerDests = workerListener.workerDests.asScala.toSeq)
      new Master(ctx)
    }

    val workerContexts: Seq[Worker.Context] = (0 until WORKER_COUNT).map(workerIndex => {
      val workerPath = PATH.resolve(s"$workerIndex")
      new Worker.Context(
        masterDest = workerListener.thisDest,
        in = workerPath.toFile.listFiles.filter(_.getName.startsWith("in")),
        out = workerPath.resolve(s"out").toFile
      )
    })
  }

  test("test") {
    new Launch {
      val workerFutures = workerContexts.map(new Worker(_)).map(worker => Future{
        worker.blockUntilShutdown()
      })

      Await.result(Future.sequence(workerFutures), Duration.Inf)
      Await.result(masterFuture, Duration.Inf)
    }
  }
}