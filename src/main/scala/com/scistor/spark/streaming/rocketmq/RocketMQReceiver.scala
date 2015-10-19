package com.scistor.spark.streaming.rocketmq

import java.util.List

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer
import com.alibaba.rocketmq.client.consumer.listener.{ ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus, MessageListenerConcurrently }
import com.alibaba.rocketmq.common.message.{ Message, MessageExt }
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
 * @author Wei Xing
 */
private[rocketmq] class RocketMQReceiver(
  nameSrvList: String,
  consumerId: String,
  topic: String,
  subExpression: String,
  storageLevel: StorageLevel)
    extends Receiver[Message](storageLevel) with Logging {
  receiver =>

  /** Thread running the worker */
  private var workerThread: Thread = null
  private var consumer: DefaultMQPushConsumer = null

  override def onStart() {
    workerThread = new Thread() {
      override def run(): Unit = {
        consumer = new DefaultMQPushConsumer(consumerId)
        consumer.setNamesrvAddr(nameSrvList);
        // 程序第一次启动从消息队列头取数据
        //      consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, subExpression)
        consumer.registerMessageListener(new MessageListenerConcurrently() {
          override def consumeMessage(messages: List[MessageExt],
            context: ConsumeConcurrentlyContext): ConsumeConcurrentlyStatus = {
            try {
              val it = messages.iterator()
              while (it.hasNext()) {
                receiver.store(it.next())
              }
              ConsumeConcurrentlyStatus.CONSUME_SUCCESS
            } catch {
              case e: Throwable =>
                ConsumeConcurrentlyStatus.RECONSUME_LATER
            }
          }
        })
        consumer.start()
      }
    }

    workerThread.setName(s"RocketMQ Receiver $streamId")
    workerThread.setDaemon(true)
    workerThread.start()

    logInfo(s"Started receiver with streamId $streamId")
  }

  override def onStop(): Unit = {
    if (workerThread != null) {
      if (consumer != null) {
        consumer.shutdown()
      }

      workerThread.join()
      workerThread = null
      logInfo(s"Stopped receiver for streamId $streamId")
    }
  }
}