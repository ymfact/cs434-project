package Common

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import protocall.MasterServiceGrpc.MasterServiceStub
import protocall.{MasterServiceGrpc, ProtoCallGrpc}
import protocall.ProtoCallGrpc.ProtoCallStub

import scala.collection.mutable

object RPCClient {
  private val channels: mutable.Map[String, ManagedChannel] = mutable.Map()
  private def getChannel(dest: String): ManagedChannel = {
    if(!channels.contains(dest)) {
      val builder = ManagedChannelBuilder.forTarget(dest)
      builder.usePlaintext()
      channels.addOne(dest -> builder.build)
    }
    channels(dest)
  }

  def master(dest: String): MasterServiceStub = {
    MasterServiceGrpc.stub(getChannel(dest)).withWaitForReady()
  }

  def worker(dest: String): ProtoCallStub = {
    ProtoCallGrpc.stub(getChannel(dest)).withWaitForReady()
  }
}
