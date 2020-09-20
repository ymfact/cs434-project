package Common

import java.io.{DataInputStream, EOFException, InputStream}

import Common.Const.{BYTE_COUNT_IN_KEY, BYTE_COUNT_IN_RECORD}
import com.google.protobuf.ByteString

import scala.annotation.tailrec

trait Record extends Ordered[Record]{
  def getKeyByte(keyIndex: Int): Byte

  override def compare(that: Record): Int = compareKey(Left(that), 0)

  def compare(that: ByteString): Int = compareKey(Right(that), 0)

  @tailrec
  private def compareKey(that: Either[Record, ByteString], checkingKeyIndex: Int): Int =
    if (checkingKeyIndex == BYTE_COUNT_IN_KEY)
      0
    else {
      val compared = getKeyByte(checkingKeyIndex).compare(that match{
        case Left(that: Record) => that.getKeyByte(checkingKeyIndex)
        case Right(that: ByteString) => that.byteAt(checkingKeyIndex)
      })
      if (compared == 0)
        compareKey(that, checkingKeyIndex + 1)
      else
        compared
    }
}

object Record {
  def from(that: Array[Byte]): RecordFromByteArray = new RecordFromByteArray(that)

  def from(that: ByteString): RecordFromByteString = new RecordFromByteString(that)

  def from(stream: DataInputStream): RecordFromByteArray = Record.from(Data.readSome(stream, BYTE_COUNT_IN_RECORD))
}