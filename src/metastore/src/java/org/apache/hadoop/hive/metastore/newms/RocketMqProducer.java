package org.apache.hadoop.hive.metastore.newms;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.message.Message;
import com.taobao.metamorphosis.exception.MetaClientException;


public class RocketMqProducer {

  private static RocketMqProducer instance = null;
  private static DefaultMQProducer producer = null;
  private static String producerGroupname = "RMQProducerGroup";
  private static String namesrvAddr = null;
  private static String topic = "meta-test";

//  private RocketMqProducer(String topic) {
//    this.topic = topic;
//    namesrvAddr = new HiveConf().getVar(ConfVars.ROCKETMQNAMESRVADDRESS);
//    producer = RocketMqProducer.getDefaultMQProducer(namesrvAddr);
//  }

  private RocketMqProducer() {
    namesrvAddr = new HiveConf().getVar(ConfVars.ROCKETMQNAMESRVADDRESS);
    producer = RocketMqProducer.getDefaultMQProducer(namesrvAddr);
  }

  private static DefaultMQProducer getDefaultMQProducer(String namesrvAddr) {
    producer = new DefaultMQProducer(producerGroupname);
    producer.setNamesrvAddr(namesrvAddr);
//    producer.setClientIP("节点的ip");
//    producer.setInstanceName(producerGroupname + "producer_instance");
    /*
     * 消息体最大不超过6M
     */
    producer.setMaxMessageSize(6 * 1024 * 1024);
    try {
      producer.start();
    } catch (MQClientException ex) {
      ex.printStackTrace();
    }
    return producer;
  }
//  public static RocketMqProducer getInstance(String topic) throws MetaClientException {
//    if(instance == null){
//      instance = new RocketMqProducer(topic);
//    }
//    return instance;
//  }
  public static RocketMqProducer getInstance() throws MetaClientException {
    if(instance == null){
      instance = new RocketMqProducer();
    }
    return instance;
  }
  public String getTopic()
  {
    return this.topic;
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
            System.err.println("send message fail one time,will sleep and retry ...");
          } else {
            System.err
                .println("send message fail one time,will sleep and retry,the information is "
                    + producer.getClientIP() + " " + producer.getProducerGroup());
          }
          try {
            Thread.sleep(200);
          } catch (Exception e) {
          }
          continue;
        } else {
          System.out.println("libing:debug,send rocketmq susccessfully,"+"id:" + sendResult.getMsgId() +
              " result:" + sendResult.getSendStatus());
          System.out.println("send to rocketmq use " + (System.currentTimeMillis() - bg) + " ms for "
              + topic);
          return true;
        }
      } catch (Exception ex) {
        System.err.println(ex + ",the information is:topic--> " + topic);
        return false;
      } finally {
      }
    }
    return false;
  }
}
