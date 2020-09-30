package Common

import java.io.OutputStream

import Common.Const.{BYTE_COUNT_IN_KEY, BYTE_OFFSET_OF_KEY}
import com.google.protobuf.ByteString

import scala.collection.View

class RecordFromByteString(raw: ByteString) extends Record {
  override protected def getKeyView: View[Byte] = Record.getKeyView(raw)

  override def copyKey: ByteString = raw.substring(BYTE_OFFSET_OF_KEY, BYTE_OFFSET_OF_KEY + BYTE_COUNT_IN_KEY)

  override def getByteArray: Array[Byte] = raw.toByteArray

  override def getByteString: ByteString = raw

  override def write(stream: OutputStream): Unit = raw.writeTo(stream)
}
