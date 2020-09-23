import java.io.File
import java.util.concurrent.Executors

import Master.Context
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import scala.collection.parallel.CollectionConverters.seqIsParallelizable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}

@RunWith(classOf[JUnitRunner])
class TestIntegration extends AnyFunSuite {
  implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor)

  trait Launch {
    val FILE_DIR = new File("D:\\works\\csed434\\project_temp\\")
    val WORKER_COUNT = 2
    val PARTITION_COUNT = 7
    val PARTITION_SIZE = 0x10000
    val SAMPLE_COUNT = 1000
    val IS_BINARY = true

    val workerContexts: Seq[Worker.Context] =
      (0 until WORKER_COUNT)
        .map(workerIndex =>
          new Worker.Context(
            rootDir = FILE_DIR,
            workerCount = WORKER_COUNT,
            workerIndex = workerIndex,
            partitionCount = PARTITION_COUNT,
            partitionSize = PARTITION_SIZE,
            sampleCount = SAMPLE_COUNT,
            isBinary = IS_BINARY)
        )
    val masterContext: Context =
      new Master.Context(
        rootDir = FILE_DIR,
        workerCount = WORKER_COUNT,
        partitionCount = PARTITION_COUNT,
        partitionSize = PARTITION_SIZE
      )
  }

  test("test") {
    new Launch {
      Future{new Master(masterContext)}
      val workers: Seq[Worker] = workerContexts.map(new Worker(ExecutionContext.global, _))
      for (worker <- workers.par) {
        worker.start()
        worker.blockUntilShutdown()
      }
    }
  }
}
