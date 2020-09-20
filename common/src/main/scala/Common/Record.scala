package Common

import java.io.{EOFException, InputStream}

import Common.Const.{BYTE_COUNT_IN_KEY, BYTE_COUNT_IN_RECORD}
import com.google.protobuf.ByteString

import scala.annotation.tailrec
import scala.util.Try

trait Record extends Ordered[Record]{

  def getKeyByteString: ByteString

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

  def toByteString: ByteString

  def toByteArray: Array[Byte]

  override def toString = new String(toByteArray)
}

object Record {
  def from(that: Array[Byte]): Record = new RecordFromByteArray(that)

  def from(that: ByteString): Record = new RecordFromByteString(that)

  def from(stream: InputStream): Record ={
    try{
      Record.from(Data.readSome(stream, BYTE_COUNT_IN_RECORD))
    }catch{
      case _: EOFException => null
    }
  }
}