package Common

import Common.Const.{BYTE_COUNT_IN_KEY, BYTE_COUNT_IN_RECORD, BYTE_OFFSET_OF_KEY}
import com.google.protobuf
import com.google.protobuf.ByteString

import scala.collection.{SeqFactory, mutable}

object RecordTypes {

  trait RecordArray[+RecordType <: RecordPtr] extends collection.Seq[RecordType]

  class ImmutableRecordArray(arr: ByteString) extends RecordArray[ImmutableRecordPtr] {

    class Iterator extends scala.Iterator[ImmutableRecordPtr] {
      var currentIndex: Int = 0

      override def hasNext: Boolean = currentIndex < ImmutableRecordArray.this.length

      override def next(): ImmutableRecordPtr = {
        val next = new ImmutableRecordPtr(arr, currentIndex)
        currentIndex += 1
        next
      }
    }

    override def iterator: scala.Iterator[ImmutableRecordPtr] = new Iterator

    implicit def toByteString: ByteString = arr

    override def apply(i: Int): ImmutableRecordPtr = new ImmutableRecordPtr(arr, i)

    override def length: Int = arr.size / BYTE_COUNT_IN_RECORD
  }

  object ImmutableRecordArray {
    implicit def from(byteString: ByteString): ImmutableRecordArray = new ImmutableRecordArray(byteString)
  }

  class MutableRecordArray(arr: Array[Byte]) extends RecordArray[MutableRecordPtr] with mutable.IndexedSeq[MutableRecordPtr] {
    override def update(idx: Int, elem: MutableRecordPtr): Unit = update(idx, elem.toArray)

    def update(idx: Int, elem: Array[Byte]): Unit = elem.copyToArray(arr, idx * BYTE_COUNT_IN_RECORD, BYTE_COUNT_IN_RECORD)

    override def apply(i: Int): MutableRecordPtr = new MutableRecordPtr(arr, i)

    override def length: Int = arr.length / BYTE_COUNT_IN_RECORD

    class Iterator extends scala.Iterator[MutableRecordPtr] {
      var currentIndex: Int = 0

      override def hasNext: Boolean = currentIndex < MutableRecordArray.this.length

      override def next(): MutableRecordPtr = {
        val next = new MutableRecordPtr(arr, currentIndex)
        currentIndex += 1
        next
      }
    }

    override def iterator: scala.Iterator[MutableRecordPtr] = new Iterator

    def toByteArray: Array[Byte] = arr

    def toByteString: ByteString = ByteString.copyFrom(arr)
  }

  object MutableRecordArray {
    implicit def from(byteString: ByteString): MutableRecordArray = new MutableRecordArray(byteString.toByteArray)

    implicit def from(array: Array[Byte]): MutableRecordArray = new MutableRecordArray(array)
  }

  trait RecordPtr extends Ordered[RecordPtr] {
    def getKeyByteString: ByteString

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

    def getByteArray: Array[Byte]

    override def toString = new String(getByteArray)
  }

  class MutableRecordPtr(val arr: Array[Byte], index: Int) extends RecordPtr {

    override def getKeyByte(keyIndex: Int): Byte = arr(index * BYTE_COUNT_IN_RECORD + BYTE_OFFSET_OF_KEY + keyIndex)

    implicit def toArray: scala.Array[Byte] = arr.slice(index * BYTE_COUNT_IN_RECORD, (index + 1) * BYTE_COUNT_IN_RECORD)

    def getByteArray: Array[Byte] = toArray

    override def getKeyByteString: ByteString =
      ByteString.copyFrom(arr.slice(index * BYTE_COUNT_IN_RECORD + BYTE_OFFSET_OF_KEY, index * BYTE_COUNT_IN_RECORD + BYTE_OFFSET_OF_KEY + BYTE_COUNT_IN_RECORD))
  }

  class ImmutableRecordPtr(arr: ByteString, index: Int) extends RecordPtr {
    override def getKeyByte(keyIndex: Int): Byte = arr.byteAt(index * BYTE_COUNT_IN_RECORD + BYTE_OFFSET_OF_KEY + keyIndex)

    implicit def toByteString[ByteString]: protobuf.ByteString = arr.substring(index * BYTE_COUNT_IN_RECORD, (index + 1) * BYTE_COUNT_IN_RECORD)

    def getByteArray: Array[Byte] = toByteString.toByteArray

    override def getKeyByteString: ByteString =
      arr.substring(index * BYTE_COUNT_IN_RECORD + BYTE_OFFSET_OF_KEY, index * BYTE_COUNT_IN_RECORD + BYTE_OFFSET_OF_KEY + BYTE_COUNT_IN_RECORD)
  }

}
