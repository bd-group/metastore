package org.apache.hadoop.hive.metastore.msg;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.message.Message;
import com.taobao.metamorphosis.exception.MetaClientException;

public class RocketMqMetaProducer {
  private static final Log LOG = LogFactory.getLog(RocketMqMetaProducer.class);
  private static RocketMqMetaProducer instance = null;
  private static DefaultMQProducer producer = null;
  private static String producerGroupname = "RMQMetaProducerGroup";
  private static String namesrvAddr = null;
  private static String topic = "meta-test";

  private RocketMqMetaProducer(String topic) {
    RocketMqMetaProducer.topic = topic;
    namesrvAddr = new HiveConf().getVar(ConfVars.ROCKETMQ_NAMESRV_ADDRESS);
    producer = RocketMqMetaProducer.getDefaultMQProducer(namesrvAddr);
  }

  private RocketMqMetaProducer() {
    namesrvAddr = new HiveConf().getVar(ConfVars.ROCKETMQ_NAMESRV_ADDRESS);
    producer = RocketMqMetaProducer.getDefaultMQProducer(namesrvAddr);
  }

  private static DefaultMQProducer getDefaultMQProducer(String namesrvAddr) {
    producer = new DefaultMQProducer(producerGroupname);
    producer.setNamesrvAddr(namesrvAddr);
    //producer.setClientIP("host ip");
    producer.setInstanceName(producerGroupname + "-oldms");
    /*
     * 消息体最大不超过6M
     */
    producer.setMaxMessageSize(6 * 1024 * 1024);
    try {
      producer.start();
    } catch (MQClientException e) {
      LOG.error(e, e);
    }
    LOG.info("Topic '" + topic + "' has been published.");

    return producer;
  }

  public static RocketMqMetaProducer getInstance(String topic) throws MetaClientException {
    if (instance == null) {
      instance = new RocketMqMetaProducer(topic);
    }
    return instance;
  }

  public static RocketMqMetaProducer getInstance() throws MetaClientException {
    if (instance == null) {
      instance = new RocketMqMetaProducer();
    }
    return instance;
  }

  public String getTopic() {
    return RocketMqMetaProducer.topic;
  }

  public boolean sendMessage(String message) {
    byte[] pData = message.getBytes();
    long bg = System.currentTimeMillis();
    SendResult sendResult = null;
    Message msg = new Message(topic, pData);

    while (sendResult == null || sendResult.getSendStatus() != SendStatus.SEND_OK) {
      try {
        sendResult = producer.send(msg);
        if (sendResult == null || sendResult.getSendStatus() == SendStatus.FLUSH_DISK_TIMEOUT) {
          if (sendResult == null) {
            LOG.error("Send msg failed: null sendResult.");
          } else {
            LOG.error("Send msg failed: " + sendResult.getSendStatus());
          }
          try {
            Thread.sleep(200);
          } catch (Exception e) {
          }
          continue;
        } else {
          LOG.debug("send to rocketmq use " + (System.currentTimeMillis() - bg) +
              " ms for " + topic);
          return true;
        }
      } catch (Exception e) {
        LOG.error(e, e);
        return false;
      } finally {
      }
    }
    return false;
  }
}
