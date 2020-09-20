package Common

import java.io.{EOFException, InputStream, OutputStream}
import java.nio.file.{Files, Path}

import Common.RecordStream.RecordStream
import com.google.protobuf.ByteString
import org.apache.logging.log4j.scala.Logging

import scala.collection.mutable

object Data extends Logging {

  def inputStream(path: Path): InputStream = Files.newInputStream(path)
  def outputStream(path: Path): OutputStream = Files.newOutputStream(path)

  def readAll(path: Path): ByteString = ByteString.readFrom(inputStream(path))

  def readSome(stream: InputStream, len: Int): Array[Byte] ={
    val buffer = Array.ofDim[Byte](len)
    if(len == stream.read(buffer, 0, len))
      buffer
    else
      throw new EOFException
  }

  def write(path: Path, data: ByteString): Unit = data.writeTo(outputStream(path))

  def sortFromSorteds(arrays: collection.Seq[RecordStream]): Iterator[RecordFromByteArray] = {
    new Iterator[RecordFromByteArray]{
      private val queue = mutable.SortedMap[RecordFromByteArray, Iterator[RecordFromByteArray]]()
      private var iter = queue.addAll(arrays.map(_.iterator).map(iter => iter.next() -> iter)).iterator

      override def hasNext: Boolean = iter.hasNext
      override def next(): RecordFromByteArray = {
        val (record, arrayIter) = iter.next()
        queue.remove(record)
        if(arrayIter.hasNext)
          queue.addOne(arrayIter.next() -> arrayIter)
        iter = queue.iterator
        record
      }
    }
  }

  def inplaceSort(data: RecordArray): RecordArray = {
    inplaceSort(data, 0, data.length - 1)
    data
  }

  private def merge(arr: RecordArray, beginLeft: Int, lastLeft: Int, lastRight: Int): Unit = {
    var currentLastLeft = lastLeft
    var currentLeft = beginLeft
    var currentRight = currentLastLeft + 1
    if (arr(currentLastLeft) <= arr(currentRight)) return
    while (currentLeft <= currentLastLeft && currentRight <= lastRight)
      if (arr(currentLeft) <= arr(currentRight))
        currentLeft += 1
      else {
        val value = arr(currentRight).toByteArray
        var index = currentRight
        while (index != currentLeft) {
          arr(index) = arr(index - 1)
          index -= 1
        }
        arr(currentLeft) = value
        currentLeft += 1
        currentLastLeft += 1
        currentRight += 1
      }
  }

  private def inplaceSort(arr: RecordArray, begin: Int, last: Int): Unit = {
    if (begin < last) {
      val mid = begin + (last - begin) / 2
      inplaceSort(arr, begin, mid)
      inplaceSort(arr, mid + 1, last)
      merge(arr, begin, mid, last)
    }
  }
}
