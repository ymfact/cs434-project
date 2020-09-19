package Common

import Common.Const.{BYTE_COUNT_IN_KEY, BYTE_COUNT_IN_RECORD, BYTE_OFFSET_OF_KEY}
import com.google.protobuf
import com.google.protobuf.ByteString

import scala.collection.mutable

object RecordTypes {

  class ImmutableRecordArray(arr: ByteString) extends Iterable[RecordPtr] {

    class Iterator extends scala.Iterator[RecordPtr] {
      var currentIndex: Int = 0

      override def hasNext: Boolean = currentIndex < arr.size() / BYTE_COUNT_IN_RECORD

      override def next(): RecordPtr = {
        val next = new ImmutableRecordPtr(arr, currentIndex)
        currentIndex += 1
        next
      }
    }

    override def iterator: scala.Iterator[RecordPtr] = new Iterator

    implicit def toByteString: ByteString = arr
  }

  object ImmutableRecordArray {
    implicit def from(byteString: ByteString): ImmutableRecordArray = new ImmutableRecordArray(byteString)
  }

  class MutableRecordArray(arr: Array[Byte]) extends mutable.IndexedSeq[MutableRecordPtr] with Iterable[RecordPtr] {
    override def update(idx: Int, elem: MutableRecordPtr): Unit = elem.toArray.copyToArray(arr, idx * BYTE_COUNT_IN_RECORD)

    override def apply(i: Int): MutableRecordPtr = new MutableRecordPtr(arr, i)

    override def length: Int = arr.length / BYTE_COUNT_IN_RECORD

    def toByteArray: Array[Byte] = arr

    def toByteString: ByteString = ByteString.copyFrom(arr)
  }

  object MutableRecordArray {
    implicit def from(byteString: ByteString): MutableRecordArray = new MutableRecordArray(byteString.toByteArray)

    implicit def from(array: Array[Byte]): MutableRecordArray = new MutableRecordArray(array)
  }

  trait RecordPtr extends Ordered[RecordPtr] {
    def getKeyByte(keyIndex: Int): Byte

    override def compare(that: RecordPtr): Int = compareKey(this, that, BYTE_OFFSET_OF_KEY)

    private def compareKey(left: RecordPtr, right: RecordPtr, checkingKeyIndex: Int): Int =
      if (checkingKeyIndex == BYTE_OFFSET_OF_KEY + BYTE_COUNT_IN_KEY)
        0
      else {
        val compared = left.getKeyByte(checkingKeyIndex).compare(right.getKeyByte(checkingKeyIndex))
        if (compared == 0)
          compareKey(left, right, checkingKeyIndex + 1)
        else
          compared
      }
  }

  class MutableRecordPtr(val arr: Array[Byte], index: Int) extends RecordPtr {
    implicit def toArray[Array[Byte]]: scala.Array[Byte] = arr.slice(index * BYTE_COUNT_IN_RECORD, (index + 1) * BYTE_COUNT_IN_RECORD)

    override def getKeyByte(keyIndex: Int): Byte = arr(index * BYTE_COUNT_IN_KEY + BYTE_OFFSET_OF_KEY + keyIndex)
  }

  class ImmutableRecordPtr(arr: ByteString, index: Int) extends RecordPtr {
    override def getKeyByte(keyIndex: Int): Byte = arr.byteAt(index * BYTE_COUNT_IN_KEY + BYTE_OFFSET_OF_KEY + keyIndex)

    implicit def toByteString[ByteString]: protobuf.ByteString = arr.substring(index * BYTE_COUNT_IN_RECORD, (index + 1) * BYTE_COUNT_IN_RECORD)
  }

}
