package Common

import java.io.{DataInputStream, EOFException}

import com.google.protobuf.ByteString

object RecordStream {
  type RecordStream = Iterator[Record]

  def from(stream: DataInputStream): RecordStream =
    new RecordStream {
      private def getNextElem: Option[Record] =
        try Some(Record.from(stream)) catch {
          case _: EOFException => None
        }

      private var nextElem: Option[Record] = getNextElem

      override def hasNext: Boolean = nextElem.isDefined

      override def next(): Record = {
        val result = nextElem.get
        nextElem = getNextElem
        result
      }
    }

  def recordsToByteString(records: Iterable[Record]): ByteString =
    records.map(_.getByteString).fold(ByteString.EMPTY)(_ concat _)
}
