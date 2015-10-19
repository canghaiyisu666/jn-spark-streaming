package com.scistor.spark.streaming.rocketmq

import com.alibaba.rocketmq.common.message.Message
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.JavaDStream

/**
 * @author Wei Xing
 */
object RocketMQUtils {
  /**
   * Create an input stream that pulls message from a RocketMQ stream.
   * @param ssc StreamingContext object
   * @param consumerId Name of a set of consumers
   * @param topic Which topic to subscribe
   * @param tags Which tag to subscribe
   * @param storageLevel Storage level to use for storing the received objects.
   *                     StorageLevel.MEMORY_AND_DISK_2 is recommended.
   * @return
   */
  def createJavaDStream(
                         ssc: StreamingContext,
                         nameSrvList: String,
                         consumerId: String,
                         topic: String,
                         tags: String,
                         storageLevel: StorageLevel): JavaDStream[Message] = {
    new RocketMQInputDStream(ssc, nameSrvList, consumerId, topic, tags, storageLevel)
  }
}
