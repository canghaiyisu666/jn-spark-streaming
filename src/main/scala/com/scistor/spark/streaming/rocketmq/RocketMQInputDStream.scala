package com.scistor.spark.streaming.rocketmq

import com.alibaba.rocketmq.common.message.Message
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

/**
 * @author Wei Xing
 */
private[rocketmq] class RocketMQInputDStream(
                                              @transient _ssc: StreamingContext,
                                              nameSrvList: String,
                                              consumerId: String,
                                              topic: String,
                                              subExpression: String,
                                              storageLevel: StorageLevel)
  extends ReceiverInputDStream[Message](_ssc) {

  override def getReceiver(): Receiver[Message] = {
    new RocketMQReceiver(nameSrvList, consumerId, topic, subExpression, storageLevel)
  }
}
