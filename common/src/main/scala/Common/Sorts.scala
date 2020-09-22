package Common

import Common.Const.BYTE_COUNT_IN_RECORD
import Common.RecordStream.RecordStream
import Common.Util.log2

import scala.collection.mutable
import scala.collection.parallel.CollectionConverters.seqIsParallelizable

object Sorts {
  def sortFromSorteds(streams: Seq[RecordStream]): Iterator[RecordFromStream] = new Iterator[RecordFromStream] {
    private var queue = mutable.SortedMap[RecordFromStream, Iterator[RecordFromStream]](streams.map(_.iterator).map(iter => iter.next() -> iter):_*)

    override def hasNext: Boolean = queue.nonEmpty

    override def next(): RecordFromStream = {
      val (record, arrayIter) = queue.head
      queue = queue.tail
      if (arrayIter.hasNext)
        queue.addOne(arrayIter.next() -> arrayIter)
      record
    }
  }

  def mergeSort(records: RecordArray): RecordArray = {
    val len = records.length
    val extraBuffer = mutable.ArrayBuffer.fill[Byte](len * BYTE_COUNT_IN_RECORD)(0)
    val extra = new RecordArray(extraBuffer)
    val stepCount = log2(len).ceil.toInt

    var srcDest = (records, extra)

    def src: RecordArray = srcDest._1

    def dest: RecordArray = srcDest._2

    for (step <- 0 until stepCount) {
      mergeSort(src, dest, step)
      srcDest = srcDest.swap
    }
    src
  }

  private def mergeSort(src: RecordArray, dest: RecordArray, step: Int): Unit = {
    val chunkSize = 1 << step
    (0 until src.length by 2 * chunkSize).par.foreach(begin => {
      val mid = src.length min (begin + chunkSize)
      val end = src.length min (begin + chunkSize + chunkSize)
      mergeSortConquer(src, dest, begin, mid, end)
    })
  }

  private def mergeSortConquer(src: RecordArray, dest: RecordArray, begin: Int, mid: Int, end: Int): Unit = {
    var leftCursor = begin
    var rightCursor = mid
    val leftEnd = mid
    val rightEnd = end
    var extraCursor = begin
    while (true) {
      if (leftCursor >= leftEnd) {
        dest.data.patchInPlace(extraCursor * BYTE_COUNT_IN_RECORD, src.data.slice(rightCursor * BYTE_COUNT_IN_RECORD, rightEnd * BYTE_COUNT_IN_RECORD), (rightEnd - rightCursor) * BYTE_COUNT_IN_RECORD)
        return
      }
      else if (rightCursor >= rightEnd) {
        dest.data.patchInPlace(extraCursor * BYTE_COUNT_IN_RECORD, src.data.slice(leftCursor * BYTE_COUNT_IN_RECORD, leftEnd * BYTE_COUNT_IN_RECORD), (leftEnd - leftCursor) * BYTE_COUNT_IN_RECORD)
        return
      }
      else if (src(leftCursor) < src(rightCursor)) {
        dest(extraCursor) = src(leftCursor)
        leftCursor += 1
      } else {
        dest(extraCursor) = src(rightCursor)
        rightCursor += 1
      }
      extraCursor += 1
    }
  }
}
