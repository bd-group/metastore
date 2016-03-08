package org.apache.hadoop.hive.metastore.newms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.taobao.metamorphosis.exception.MetaClientException;


public class RocketMqProducer {
  private static final Log LOG = LogFactory.getLog(RocketMqProducer.class);
  private static RocketMqProducer instance = null;
  private static DefaultMQProducer producer = null;
  private static String producerGroupname = "RMQProducerGroup";
  private static String namesrvAddr = null;
  private static String topic = "meta-test";

  private RocketMqProducer() {
    namesrvAddr = new HiveConf().getVar(ConfVars.ROCKETMQ_NAMESRV_ADDRESS);
    producer = RocketMqProducer.getDefaultMQProducer(namesrvAddr);
  }

  private static DefaultMQProducer getDefaultMQProducer(String namesrvAddr) {
    producer = new DefaultMQProducer(producerGroupname);
    producer.setNamesrvAddr(namesrvAddr);
    //producer.setClientIP("节点的ip");
    producer.setInstanceName(producerGroupname + "-newms");
    producer.setSendMsgTimeout(5 * 1000);
    /*
     * 消息体最大不超过6M
     */
    producer.setMaxMessageSize(6 * 1024 * 1024);
    try {
      producer.start();
    } catch (MQClientException ex) {
      ex.printStackTrace();
    }
    LOG.info("Topic '" + topic + "' has been published.");

    return producer;
  }

  public static RocketMqProducer getInstance() throws MetaClientException {
    if(instance == null){
      instance = new RocketMqProducer();
    }
    return instance;
  }

  public String getTopic() {
    return RocketMqProducer.topic;
  }

  public boolean sendMessage(String message) {
    byte[] pData = message.getBytes();
    long bg = System.currentTimeMillis();
    SendResult sendResult = null;
    Message msg = new Message(topic, pData);

    try {
      sendResult = producer.send(msg);
    } catch (MQClientException e) {
      LOG.error(e, e);
    } catch (RemotingException e) {
      LOG.error(e, e);
    } catch (MQBrokerException e) {
      LOG.error(e, e);
    } catch (InterruptedException e) {
      LOG.error(e, e);
    }

    if (sendResult == null || sendResult.getSendStatus() != SendStatus.SEND_OK) {
      if (sendResult == null) {
        LOG.error("Send msg failed: null sendResult.");
      } else {
        LOG.error("Send msg failed: " + sendResult.getSendStatus());
      }
      return false;
    } else {
      LOG.debug("send to rocketmq use " + (System.currentTimeMillis() - bg) +
          " ms for " + topic);
      return true;
    }
  }
}
