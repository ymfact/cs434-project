package Common

import java.io.InputStream
import java.nio.file.{Files, Path}

import Common.Const.{BYTE_COUNT_IN_KEY, BYTE_COUNT_IN_RECORD, BYTE_OFFSET_OF_KEY}
import com.google.protobuf.ByteString

object Data {

  private def stream(path: Path): InputStream = Files.newInputStream(path)

  def readAll(path: Path): ByteString = ByteString.readFrom(stream(path))

  def readSome(path: Path, len: Int): ByteString = ByteString.copyFrom(LazyList.continually(stream(path).read).map(_.toByte).take(len).toArray)

  def sort(data: ByteString): ByteString = {
    data
  }

  private def compareByteString(left: ByteString, right: ByteString): Boolean =
    compareKey(left.substring(BYTE_OFFSET_OF_KEY, BYTE_COUNT_IN_KEY), right.substring(BYTE_OFFSET_OF_KEY, BYTE_COUNT_IN_KEY))

  private def compareKey(left: ByteString, right:ByteString): Boolean =
    if(left.isEmpty && right.isEmpty)
      false
    else {
      val leftHead = left.byteAt(0)
      val rightHead = right.byteAt(0)
      if (leftHead == rightHead)
        compareKey(left.substring(1), right.substring(1))
      else
        leftHead > rightHead
    }
}
