package org.apache.hadoop.hive.metastore.msg;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.api.FileOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.msg.MSGFactory.DDLMsg;
import org.apache.hadoop.hive.metastore.tools.HiveMetaTool;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.client.MessageSessionFactory;
import com.taobao.metamorphosis.client.MetaClientConfig;
import com.taobao.metamorphosis.client.MetaMessageSessionFactory;
import com.taobao.metamorphosis.client.consumer.ConsumerConfig;
import com.taobao.metamorphosis.client.consumer.MessageConsumer;
import com.taobao.metamorphosis.client.consumer.MessageListener;
import com.taobao.metamorphosis.client.producer.MessageProducer;
import com.taobao.metamorphosis.client.producer.SendResult;
import com.taobao.metamorphosis.exception.MetaClientException;
import com.taobao.metamorphosis.utils.ZkUtils.ZKConfig;

public class MetaMsgServer {

  public static final Log LOG = LogFactory.getLog(ObjectStore.class.getName());
  static String zkAddr = "127.0.0.1:3181";
  static Producer producer =  null;
  static RocketMqMetaProducer rmProducer = null;
  static int times = 3;
  static MetaMsgServer server = null;
  private static boolean initalized = false;
  private static SendThread send = new SendThread();
  private static boolean zkfailed = false;
  private static long sleepSeconds = 60l;
  static ConcurrentLinkedQueue<DDLMsg> queue = new ConcurrentLinkedQueue<DDLMsg>();

  //add for message queue select
  private static boolean isRocketMqSelected = new HiveConf().getBoolVar(ConfVars.IS_ROCKETMQ_SELECTED);

  private static ConcurrentLinkedQueue<DDLMsg> failed_queue = new ConcurrentLinkedQueue<DDLMsg>();

  static{
    send.start();
//    try {
//			new AsyncConsumer("meta-newms","meta-metastore").consume();
//		} catch (MetaClientException e) {
//			LOG.error(e, e);
//		}
  }

  private  static void initalize(String topic) throws MetaClientException {
    server = new MetaMsgServer();
    if(!isRocketMqSelected)
    {
      Producer.config(zkAddr, topic);
    producer = Producer.getInstance();
    initalized = true;
    zkfailed = false;
    LOG.info("libing:debug,MetaMsgServer.initalize Producer:"+Producer.class.toString()+" and topic is:"+topic);
    }
    else {
      rmProducer = RocketMqMetaProducer.getInstance(topic);
      initalized = true;
      zkfailed = false;
      LOG.info("libing:debug,MetaMsgServer.initalize rmProducer:"+RocketMqMetaProducer.class.toString()+" and topic is:"+topic);
    }

  }

  private static void reconnect() throws MetaClientException {
    if(!isRocketMqSelected)
    {
      Producer.config(zkAddr, Producer.topic);
      producer = Producer.getInstance();
      initalized = true;
      zkfailed = false;
    }
    else {
      rmProducer = RocketMqMetaProducer.getInstance();
      initalized = true;
      zkfailed = false;
    }


  }

  public static void start(String topic) throws MetaClientException{
    if (!initalized){
      initalize(topic);
    }
  }

  public static void sendMsg(DDLMsg msg) {
    queue.add(msg);
    send.release();
  }

  //zy  是不是应该release多个
  public static void sendMsg(List<DDLMsg> msgs) {
    queue.addAll(msgs);
    send.release();
  }

  public static class SendThread extends Thread{
    private static final int MSG_SEND_BATCH=0;
    Semaphore sem  = new Semaphore(MSG_SEND_BATCH);

    @Override
    public void run() {
      while(true ){
        try{
          if(queue.isEmpty()){
            LOG.debug("---in sendThread before ac");
            sem.acquire();
            LOG.debug("---in sendThread after ac");
            if (queue.isEmpty()) {
              continue;
            }
          }

          if (zkfailed) {
            try {
              Thread.sleep(sleepSeconds * 1000l);
              reconnect();
            } catch(InterruptedException e) {
            } catch(MetaClientException e){
              zkfailed = true;
            }
          }
          DDLMsg msg = queue.peek();
          boolean succ = sendDDLMsg(msg);
          if (!succ) {
            if (!failed_queue.contains(msg)) {
              failed_queue.add(msg);
            }
          } else{
            failed_queue.remove(queue.poll());

            if (!failed_queue.isEmpty()) {
              int i = 0;
//              while(i++ < MSG_SEND_BATCH && !failed_queue.isEmpty()){//retry send faild msg
              while( !failed_queue.isEmpty()){//retry send faild msg,old msg should send as soon as possible.
                DDLMsg retry_msg =failed_queue.peek();
                if (!sendDDLMsg(retry_msg)) {
                  break;
                } else {
                  failed_queue.poll();
                }
              }
            }
          }
        } catch (Exception e) {
          LOG.error(e,e);
        }
      }
    }

    public void release(){
      sem.release();
    }
  }


  public static String getZkAddr() {
    return zkAddr;
  }

  public static void setZkAddr(String zkAddr) {
    MetaMsgServer.zkAddr = zkAddr;
  }

  public static boolean  sendDDLMsg(DDLMsg msg) {
    String jsonMsg = "";

    jsonMsg = MSGFactory.getMsgData(msg);
    LOG.info("---zjw-- send ddl msg to topic=" + Producer.topic + ":" + jsonMsg);
    boolean success = false;

    success = retrySendMsg(jsonMsg, times);
    return success;
  }

  private static boolean retrySendMsg(String jsonMsg,int times){
    // FIXME: if server not initialized, just return true;
    if (!initalized) {
      return true;
    }
    if(times <= 0){
      zkfailed = true;
      return false;
    }

    boolean success = false;
    try{
      if(!isRocketMqSelected)
      {
        LOG.info("libing,debug:MetaMsgServer sendMsg by which producer:"+producer.getClass().toString()+" the  topic is :"+ producer.topic);
        success = producer.sendMsg(jsonMsg);
      }
      else {
        LOG.info("libing,debug:debug:MetaMsgServer sendMsg by which producer:"+rmProducer.getClass().toString()+" the rm topic is :"+ rmProducer.getTopic());
        success = rmProducer.sendMessage(jsonMsg);
      }


    }catch(InterruptedException ie){
      LOG.error(ie,ie);
      return retrySendMsg(jsonMsg,times-1);
    } catch (MetaClientException e) {
      LOG.error(e,e);
      return retrySendMsg(jsonMsg,times-1);
    }
    // FIXME: BUG-XXX: handle timeout exception here !
    return success;
  }

  public static class AsyncConsumer {
    final MetaClientConfig metaClientConfig = new MetaClientConfig();
    final ZKConfig zkConfig = new ZKConfig();
    private ThriftHiveMetastore.Client client = null;
    private final HiveConf hiveConf = new HiveConf(HiveMetaTool.class);
    private final ObjectStore ob;
    final String topic ;
    final String group ;
    public AsyncConsumer(String topic, String group) {
    	this.topic = topic;
    	this.group = group;
    	ob = new ObjectStore();
    	ob.setConf(hiveConf);
    }
    private ThriftHiveMetastore.Client createNewMSClient() throws TTransportException
    {
    	String[] uri =hiveConf.get("newms.rpc.uri").split(":");
    	TTransport tt = new TSocket(uri[0], Integer.parseInt(uri[1]));
    	tt.open();
    	TProtocol protocol = new TBinaryProtocol(tt);
    	client = new ThriftHiveMetastore.Client(protocol);

    	return client;
    }

    public void consume() throws MetaClientException{
      //设置zookeeper地址
      zkConfig.zkConnect = MetaMsgServer.zkAddr;

      metaClientConfig.setZkConfig(zkConfig);
      // New session factory,强烈建议使用单例
      MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(metaClientConfig);

      // create consumer,强烈建议使用单例

      //生成处理线程
      MessageConsumer consumer =
      sessionFactory.createConsumer(new ConsumerConfig(group));
      //订阅事件，MessageListener是事件处理接口
      LOG.info("libing:debug,MetaMsgServer.AsyncConsumer subscribe topic is :"+ topic);
      consumer.subscribe(topic, 1024, new MessageListener(){

        @Override
        public Executor getExecutor() {
          return null;
        }

        @Override
        public void recieveMessages(final Message message) {
          try {
          	if(client == null) {
              client  = createNewMSClient();
            }
					} catch (TTransportException e) {
						LOG.error(e, e);
						client = null;
						throw new RuntimeException(e.getMessage());
					}
          String data = new String(message.getData());
          LOG.info("---zy--consume msg: " + data);
//          System.out.println(data);
          DDLMsg msg = DDLMsg.fromJson(data);

          int event_id = (int) msg.getEvent_id();
          switch(event_id){
	          case MSGType.MSG_REP_FILE_CHANGE:
	          case MSGType.MSG_STA_FILE_CHANGE:
	          case MSGType.MSG_REP_FILE_ONOFF:
	          	break;
	          case MSGType.MSG_CREATE_FILE:
	          {
	          	long fid = Long.parseLong(msg.getMsg_data().get("f_id").toString());
	          	try {
								SFile sf = client.get_file_by_id(fid);
								ob.persistFile(sf);
							} catch(InvalidObjectException e){
								LOG.error(e,e);
							} catch (FileOperationException e) {
								LOG.error(e,e);
							} catch (MetaException e) {
								LOG.error(e,e);
								throw new RuntimeException(e.getMessage());
							} catch (TException e) {
								LOG.error(e,e);
								throw new RuntimeException(e.getMessage());
							}

	          	break;
	          }
	          case MSGType.MSG_DEL_FILE:
	          {
	          	long fid = Long.parseLong(msg.getMsg_data().get("f_id").toString());
	          	try {
	          		SFile sf = ob.getSFile(fid);
	          		if(sf == null) {
                  break;
                }
	          		if(sf.getLocations() != null) {
                  for(SFileLocation sfl : sf.getLocations()) {
                    ob.delSFileLocation(sfl.getDevid(), sfl.getLocation());
                  }
                }
								ob.delSFile(fid);
							} catch (MetaException e) {
								LOG.error(e,e);
								throw new RuntimeException(e.getMessage());
							}
	          	break;
	          }

	          default:
	          {
	          	LOG.warn("unhandled msg:"+msg.getEvent_id());
	          	break;
	          }
          }
//          if(msg.getLocalhost_name().equals(localhost_name))
//          {
//            LOG.info("---zy--local msg,no need to refresh " );
////            handler.refresh(msg);
//          }
//          else
          //just test
//          handler.refresh(msg);
        }

      }
      );
      consumer.completeSubscribe();

      LOG.info("---zy-- consumer start at "+zkConfig.zkConnect);
    }
  }

  public static class Producer {
    private static Producer instance= null;
    private final MetaClientConfig metaClientConfig = new MetaClientConfig();
    private final ZKConfig zkConfig = new ZKConfig();
    private MessageSessionFactory sessionFactory = null;
    // create producer,强烈建议使用单例
    private MessageProducer producer = null;
    // publish topic
    private static String topic = "meta-test";
    private static String  zkAddr = "127.0.0.1:3181";

    public static void config(String addr, String t){
      topic = t;
      zkAddr = addr;
    }

    private Producer() {
        //设置zookeeper地址
        zkConfig.zkConnect = zkAddr;
        metaClientConfig.setZkConfig(zkConfig);
        // New session factory,强烈建议使用单例
        connect();
    }

    private void connect() {
      try {
        sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
        producer = sessionFactory.createProducer();
        producer.setTransactionTimeout(30 * 1000);
        LOG.info("libing:debug,MetaMsgServer:new producer() and the topic is :"+ topic);
        producer.publish(topic);
      } catch(MetaClientException e){
        LOG.error(e.getMessage());
      }
      LOG.info("Topic '" + topic + "' has been published.");
    }

    public static Producer getInstance() throws MetaClientException {
      if (instance == null){
        instance = new Producer();
      }
      return instance;
    }

    boolean sendMsg(String msg) throws MetaClientException, InterruptedException {
        LOG.debug("in send msg:"+msg);

        if(producer == null){
          connect();
          if(producer == null){
            return false;
          }
        }
        SendResult sendResult = producer.sendMessage(new Message(topic, msg.getBytes()));
        // check result

        boolean success = sendResult.isSuccess();
        if (!success) {
            LOG.error("Send message failed,error message:" + sendResult.getErrorMessage());
        }
        else {
            LOG.debug("Send message successfully,sent to " + sendResult.getPartition());
        }
        return success;
    }
  }

  public static void main(String[] args){
  }
}
