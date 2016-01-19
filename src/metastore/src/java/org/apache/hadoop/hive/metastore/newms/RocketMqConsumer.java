package org.apache.hadoop.hive.metastore.newms;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.DiskManager;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.msg.MSGFactory.DDLMsg;
import org.apache.hadoop.hive.metastore.msg.MSGType;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

public class RocketMqConsumer {
  private static final Log LOG = LogFactory.getLog(RocketMqConsumer.class);
  private static String consumerGroup = "RMQConsumerGroup";
  private static RocketMqConsumer instance = null;
  private static DefaultMQPushConsumer consumer = null;
  private static String topic = null;
  private static String nameSrvAddr = null;
  private static MsgProcessing mp;
  private boolean isNotified = false;
  private final ConcurrentLinkedQueue<DDLMsg> failedq = new ConcurrentLinkedQueue<DDLMsg>();

  private RocketMqConsumer(String nameSrvAddr,String topic) throws MetaException, IOException
  {
    this.topic = topic;
    this.nameSrvAddr = nameSrvAddr;
    consumer = new DefaultMQPushConsumer(consumerGroup);
    mp = new MsgProcessing();

  }
  public static RocketMqConsumer getInstance(String nameSrvAddr ,String topic) throws MetaException, IOException
  {
    if (instance == null) {
      instance = new RocketMqConsumer( nameSrvAddr,topic);
    }
    return instance;
  }
  public void startMsgProcessing() throws Exception {
    mp.getAllObjects();
    synchronized (mp) {
      isNotified = true;
      mp.notifyAll();
    }
  }
  public void consume() throws MQClientException{
    consumer.setNamesrvAddr(nameSrvAddr);
//    consumer.setClientIP("本机ip");
//    consumer.setInstanceName(consumerGroup + "ConsumeIns");/*消费者实例的名字，一个消费者组子啊可以有多个实例，实例的需要名字不同*/

    /*消费的最大、最小并发线程数*/
    consumer.setConsumeThreadMin(4);
    consumer.setConsumeThreadMax(4);

    /*设置 callback的并发数*/
    consumer.setClientCallbackExecutorThreads(16);
    /*
     * 从上次开始消费的位置消费; 如果是本消费者第一次启动，从最新位置开始消费，历史数据不能消费到
     */
    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
    consumer.setConsumeMessageBatchMaxSize(8);
    consumer.setPullBatchSize(8);
    consumer.setPullThresholdForQueue(32);
    consumer.setConsumeConcurrentlyMaxSpan(1024);
    /*
     * 订阅的topic，可以订阅多个topic
     */
    consumer.subscribe(topic, "*");

    consumer.registerMessageListener(new MessageListenerConcurrently() {  //注册消费线程
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context) {
            for (MessageExt message : msgs) {
                /*
                 * 得到二进制数据进行处理（反序列化、业务逻辑等）
                 */

              if (!isNotified) {
                synchronized (mp) {
                  while (true) {
                    try {
                      mp.wait();
                      break;
                    } catch (InterruptedException e) {
                    }
                  }
                }
              }
              String data = new String(message.getBody());
              if (DiskManager.role != null) {
                switch (DiskManager.role) {
                case MASTER:
                  LOG.info("Consume msg from rmq's topic=" + topic + ": " + data);
                  break;
                case SLAVE:
                default:
                  LOG.info("Slave ignore consume msg from rmq's topic=" + topic + ": " + data);
                  return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
              }
              int time = 0;
              DDLMsg msg = DDLMsg.fromJson(data);
              // FIXME: if we are in IS_OLD_WITH_NEW mode, we can ignore file-related ops
              if (new HiveConf().getBoolVar(ConfVars.NEWMS_IS_OLD_WITH_NEW) && (
                  msg.getEvent_id() == MSGType.MSG_CREATE_FILE ||
                  msg.getEvent_id() == MSGType.MSG_DEL_FILE ||
                  msg.getEvent_id() == MSGType.MSG_REP_FILE_CHANGE ||
                  msg.getEvent_id() == MSGType.MSG_REP_FILE_ONOFF ||
                  msg.getEvent_id() == MSGType.MSG_STA_FILE_CHANGE ||
                  msg.getEvent_id() == MSGType.MSG_FILE_USER_SET_REP_CHANGE ||
                  msg.getEvent_id() == MSGType.MSG_FAIL_NODE ||
                  msg.getEvent_id() == MSGType.MSG_BACK_NODE
                  // BUG-XXX: if we ignore CREATE_DEVICE and DEL_DEVICE, we can NOT
                  // handle online/offline device
                  //msg.getEvent_id() == MSGType.MSG_CREATE_DEVICE ||
                  //msg.getEvent_id() == MSGType.MSG_DEL_DEVICE
                  )) {
                // Resend by NewMS from OldMS to meta-test topic
                MsgServer.pdSend(msg);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
              }

              while (time <= 3) {
                if (time >= 3) {
                  failedq.add(msg);
                  LOG.info("handle msg failed, add msg into failed queue: " + msg.getMsg_id());
                  break;
                }
                try {
                  mp.handleMsg(msg);
                  if (!failedq.isEmpty()) {
                    msg = failedq.poll();
                    LOG.info("handle msg in failed queue: "+ msg.getMsg_id());
                    time = 0;
                  } else {
                    // 能到else一定是handlemsg没抛异常成功返回，而failedq是空的
                    break;
                  }
                } catch (Exception e) {
                  time++;
                  try {
                    Thread.sleep(1 * 1000);
                  } catch (InterruptedException e2) {
                  }
                  LOG.error(e,e);
                }
              }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    });
    consumer.start();

    System.out.println("libing,debug:rmqConsumer Started.");
  }
}
