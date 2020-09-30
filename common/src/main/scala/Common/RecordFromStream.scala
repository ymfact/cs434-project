package Common

import java.io.OutputStream

import Common.Const.{BYTE_COUNT_IN_KEY, BYTE_OFFSET_OF_KEY}
import com.google.protobuf.ByteString

import scala.collection.View

class RecordFromStream(raw: Array[Byte]) extends Record {
  override protected def getKeyView: View[Byte] = Record.getKeyView(raw.view)

  override def copyKey: ByteString = ByteString.copyFrom(raw, BYTE_OFFSET_OF_KEY, BYTE_COUNT_IN_KEY)

  override def getByteArray: Array[Byte] = raw

  override def getByteString: ByteString = ByteString.copyFrom(raw)

  override def write(stream: OutputStream): Unit = stream.write(raw)
}
