package Common

import Common.Const.{BYTE_COUNT_IN_KEY, BYTE_COUNT_IN_RECORD, BYTE_OFFSET_OF_KEY}
import com.google.protobuf.ByteString

class RecordArrayPtr(val arr: Array[Byte], index: Int) extends Record {
  override def getKeyByte(keyIndex: Int): Byte = arr(index * BYTE_COUNT_IN_RECORD + BYTE_OFFSET_OF_KEY + keyIndex)

  override def getKeyByteString: ByteString =
    ByteString.copyFrom(arr.slice(index * BYTE_COUNT_IN_RECORD + BYTE_OFFSET_OF_KEY, index * BYTE_COUNT_IN_RECORD + BYTE_OFFSET_OF_KEY + BYTE_COUNT_IN_KEY))

  override def toByteArray: scala.Array[Byte] = arr.slice(index * BYTE_COUNT_IN_RECORD, (index + 1) * BYTE_COUNT_IN_RECORD)

  override def toByteString: ByteString = ByteString.copyFrom(arr, index * BYTE_COUNT_IN_RECORD, (index + 1) * BYTE_COUNT_IN_RECORD)
}

class RecordFromByteString(byteString: ByteString) extends Record {
  override def getKeyByte(keyIndex: Int): Byte = byteString.byteAt(BYTE_OFFSET_OF_KEY + keyIndex)

  override def getKeyByteString: ByteString = byteString.substring(BYTE_OFFSET_OF_KEY, BYTE_OFFSET_OF_KEY + BYTE_COUNT_IN_KEY)

  override def toByteArray: Array[Byte] = byteString.toByteArray

  override def toByteString: ByteString = byteString
}

class RecordFromByteArray(arr: Array[Byte]) extends Record {
  override def getKeyByte(keyIndex: Int): Byte = arr(BYTE_OFFSET_OF_KEY + keyIndex)

  override def getKeyByteString: ByteString = ByteString.copyFrom(arr.slice(BYTE_OFFSET_OF_KEY, BYTE_OFFSET_OF_KEY + BYTE_COUNT_IN_KEY))

  override def toByteArray: Array[Byte] = arr

  override def toByteString: ByteString = ByteString.copyFrom(arr)
}
