package Common

import java.io.InputStream
import java.nio.file.{Files, Path}

import Common.Const.BYTE_COUNT_IN_RECORD
import Common.RecordTypes.{MutableRecordArray, RecordPtr}
import com.google.protobuf.ByteString

import scala.collection.parallel.ParIterable

object Data {

  private def stream(path: Path): InputStream = Files.newInputStream(path)

  def readAll(path: Path): ByteString = ByteString.readFrom(stream(path))

  def readSome(path: Path, len: Int): ByteString = ByteString.copyFrom(LazyList.continually(stream(path).read).map(_.toByte).take(len).toArray)

  def sortFromSorteds[RecordArrayType <: Iterable[RecordPtr]](data: Iterable[RecordArrayType]): ParIterable[MutableRecordArray] = ???
  
  def inplaceSort(data: MutableRecordArray): MutableRecordArray = {
    inplaceSort(data, 0, data.length / BYTE_COUNT_IN_RECORD)
    data
  }

  private def merge(arr: MutableRecordArray, beginLeft: Int, lastLeft: Int, lastRight: Int): Unit = {
    var currentLastLeft = lastLeft
    var currentLeft = beginLeft
    var currentRight = currentLastLeft + 1
    if (arr(currentLastLeft) <= arr(currentRight)) return
    while ( {
      currentLeft <= currentLastLeft && currentRight <= lastRight
    }) {
      if (arr(currentLeft) <= arr(currentRight)) currentLeft += 1
      else {
        val value = arr(currentRight)
        var index = currentRight
        while ( {
          index != currentLeft
        }) {
          arr(index) = arr(index - 1)
          index -= 1
        }
        arr(currentLeft) = value
        currentLeft += 1
        currentLastLeft += 1
        currentRight += 1
      }
    }
  }

  private def inplaceSort(arr: MutableRecordArray, begin: Int, last: Int): Unit = {
    if (begin < last) {
      val mid = begin + (last - begin) / 2
      inplaceSort(arr, begin, mid)
      inplaceSort(arr, mid + 1, last)
      merge(arr, begin, mid, last)
    }
  }
}
