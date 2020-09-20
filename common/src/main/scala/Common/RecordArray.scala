package Common

import Common.Const.{BYTE_COUNT_IN_KEY, BYTE_COUNT_IN_RECORD, BYTE_OFFSET_OF_KEY}
import com.google.protobuf.ByteString

import scala.collection.mutable

class RecordArray(arr: Array[Byte]) extends mutable.Seq[RecordArrayPtr] {
  override def update(idx: Int, elem: RecordArrayPtr): Unit = update(idx, elem.toByteArray)

  def update(idx: Int, elem: Array[Byte]): Unit = elem.copyToArray(arr, idx * BYTE_COUNT_IN_RECORD, BYTE_COUNT_IN_RECORD)

  override def apply(i: Int): RecordArrayPtr = new RecordArrayPtr(arr, i)

  override def length: Int = arr.length / BYTE_COUNT_IN_RECORD

  class Iterator extends scala.Iterator[RecordArrayPtr] {
    var currentIndex: Int = 0

    override def hasNext: Boolean = currentIndex < RecordArray.this.length

    override def next(): RecordArrayPtr = {
      val next = new RecordArrayPtr(arr, currentIndex)
      currentIndex += 1
      next
    }
  }

  override def iterator: scala.Iterator[RecordArrayPtr] = new Iterator

  def toByteArray: Array[Byte] = arr

  def toByteString: ByteString = ByteString.copyFrom(arr)
}

object RecordArray {
  implicit def from(byteString: ByteString): RecordArray = new RecordArray(byteString.toByteArray)

  implicit def from(array: Array[Byte]): RecordArray = new RecordArray(array)
}
