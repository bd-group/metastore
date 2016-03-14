package org.apache.hadoop.hive.metastore.newms;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

import javax.jdo.PersistenceManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.DiskManager;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.api.Device;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.hadoop.hive.metastore.msg.MSGFactory;
import org.apache.hadoop.hive.metastore.msg.MSGFactory.DDLMsg;
import org.apache.hadoop.hive.metastore.msg.MSGType;

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

public class MsgServer {
	private static final Log LOG = LogFactory.getLog(MsgServer.class);
	static Producer producer = null;
	static int times = 3;
	private static boolean initalized = false;
	private static HiveConf conf = new HiveConf();
	private static SendThread send = new SendThread();
	private static boolean zkfailed = false;
	private static long max_msg_id = 0;
	private static ConcurrentLinkedQueue<DDLMsg> queue = new ConcurrentLinkedQueue<DDLMsg>();
	private static ConcurrentLinkedQueue<DDLMsg> failed_queue = new ConcurrentLinkedQueue<DDLMsg>();
	private static ConcurrentLinkedQueue<DDLMsg> localQueue = new ConcurrentLinkedQueue<DDLMsg>();
	private static LocalConsumer lc = new LocalConsumer();

	public static boolean isQueueEmpty() {
		LOG.info("Queue size " + queue.size() + ", failed_queue " + failed_queue.size() +
		    ", localQueue " + localQueue.size());
		return queue.isEmpty() && failed_queue.isEmpty() && localQueue.isEmpty();
	}

	public static long getLocalQueueSize() {
	  return localQueue.size();
	}

	public static long getQueueSize() {
	  return queue.size();
	}

	public static long getFailedQueueSize() {
	  return failed_queue.size();
	}

	public static void pdSend(DDLMsg msg) {
	  if (initalized) {
	    // BUG-XXX: if we are in slave mode, do NOT send message to meta-test?
	    if (DiskManager.role == DiskManager.Role.MASTER) {
	      queue.add(msg);
	      send.release();
	    } else {
	      LOG.info("SLAVE ignore ddl msg to topic=" + Producer.topic + ":" +
	          MSGFactory.getMsgData2(msg));
	    }
	  }
	}
	public static void addMsg(DDLMsg msg) {
		//用来给本地消费
		int eventid = (int) msg.getEvent_id();
		switch(eventid){
		case MSGType.MSG_FILE_USER_SET_REP_CHANGE:
		case MSGType.MSG_REP_FILE_CHANGE:
		case MSGType.MSG_REP_FILE_ONOFF:
		case MSGType.MSG_STA_FILE_CHANGE:
		case MSGType.MSG_CREATE_FILE:
		case MSGType.MSG_DEL_FILE:
		case MSGType.MSG_FAIL_NODE:
    case MSGType.MSG_BACK_NODE:
    case MSGType.MSG_CREATE_DEVICE:
    case MSGType.MSG_DEL_DEVICE:
			localQueue.add(msg);
			lc.release();
		}
	}

	public static void startProducer() throws MetaClientException {
		if (!initalized) {
			initalize();
			new Thread(send).start();
		}
	}

	public static void startConsumer(String zkaddr, String topic, String group) throws Exception
	{
		Consumer c = new Consumer(zkaddr, topic, group);
		// Note-XXX: for old2new process, do NOT consume msg, otherwise, message in oldms
		// will be consumed.
		if (conf.getBoolVar(ConfVars.NEWMS_CONSUME_MSG)) {
		  c.consume();
		}
		c.startMsgProcessing();
	}

	public static void startLocalConsumer()
	{
		new Thread(lc).start();
	}

	private static void initalize() throws MetaClientException {
//		Producer.config(zkAddr);
		producer = Producer.getInstance();
		initalized = true;
		zkfailed = false;

	}

	private static void reconnect() throws MetaClientException {
//		Producer.config(zkAddr);
		producer = Producer.getInstance();
		initalized = true;
		zkfailed = false;
	}

	private static boolean sendDDLMsg(DDLMsg msg) {
		String jsonMsg = "";

		jsonMsg = MSGFactory.getMsgData2(msg);
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
      success = producer.sendMsg(jsonMsg);
    }catch(InterruptedException ie){
      return retrySendMsg(jsonMsg,times-1);
    } catch (MetaClientException e) {
      LOG.error(e,e);
      return retrySendMsg(jsonMsg,times-1);
    }
    return success;
  }

	static class SendThread implements Runnable{
		Semaphore sem  = new Semaphore(0);
		public SendThread() {
		}

		public void release(){
      sem.release();
    }
		@Override
		public void run() {
			 while(true ){
	        try{
	          if(queue.isEmpty()){
	            sem.acquire();
	            if(queue.isEmpty()){
	              continue;
	            }
	          }

	          if(zkfailed)
	          {
	            try{
	              Thread.sleep(1*1000l);
	              reconnect();
	            }catch(InterruptedException e)
	            {
	            }catch(MetaClientException e){
	              zkfailed = true;
	            }

	          }
	          DDLMsg msg = queue.peek();
	          boolean succ = sendDDLMsg(msg);
	          if(!succ){
	            if(!failed_queue.contains(msg)) {
	              failed_queue.add(msg);
	            }
	          }else{

	            failed_queue.remove(queue.poll());

	            if(!failed_queue.isEmpty()){
	              while( !failed_queue.isEmpty()){//retry send faild msg,old msg should send as soon as possible.
	                DDLMsg retry_msg =failed_queue.peek();
	                if(!sendDDLMsg(retry_msg)){
	                  break;
	                }else{
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

	}

	public static class Producer
	{
		private static Producer instance= null;
    private final MetaClientConfig metaClientConfig = new MetaClientConfig();
    private final ZKConfig zkConfig = new ZKConfig();
    private MessageSessionFactory sessionFactory = null;
    // create producer,强烈建议使用单例
    private MessageProducer producer = null;
    // publish topic
    private static String topic = "meta-test";
    private static String  zkAddr = conf.getVar(ConfVars.ZOOKEEPERADDRESS);

    private Producer() {
        //设置zookeeper地址
        zkConfig.zkConnect = zkAddr;
        metaClientConfig.setZkConfig(zkConfig);
        // New session factory,强烈建议使用单例
        connect();
    }

    private void connect(){
      try{
        sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
        producer = sessionFactory.createProducer();
        producer.publish(topic);
      }
      catch(MetaClientException e){
//        LOG.error(e.getMessage());
      	LOG.error(e,e);
      }
      LOG.info("Topic '" + topic + "' has been published.");
    }

    public static Producer getInstance() throws MetaClientException {
      if(instance == null){
        instance = new Producer();
      }
      return instance;
    }

    boolean sendMsg(String msg) throws MetaClientException, InterruptedException{
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
            LOG.debug("Send message failed,error message:" + sendResult.getErrorMessage());
        }
        else {
            LOG.debug("Send message successfully,sent to " + sendResult.getPartition());
        }
        return success;
    }
	}


	public static class Consumer {
		final MetaClientConfig metaClientConfig = new MetaClientConfig();
		final ZKConfig zkConfig = new ZKConfig();
		private String localhost_name;
		private final String zkaddr;
		private final String topic;
		private final String group;
		private final ConcurrentLinkedQueue<DDLMsg> failedq = new ConcurrentLinkedQueue<DDLMsg>();
		private final MsgProcessing mp;
		private boolean isNotified = false;

		public Consumer(String zkaddr, String topic, String group) throws MetaException, IOException {
			this.zkaddr = zkaddr;
			this.topic = topic;
			this.group = group;
			try {
				localhost_name = InetAddress.getLocalHost().getHostName();
			} catch (UnknownHostException e) {
				LOG.error(e,e);
			}
			mp = new MsgProcessing();
		}

		public void startMsgProcessing() throws Exception {
		  mp.getAllObjects();
		  synchronized (mp) {
		    isNotified = true;
		    mp.notifyAll();
		  }
		}

		public void consume() throws MetaClientException {
			// 设置zookeeper地址
			zkConfig.zkConnect = zkaddr;
			metaClientConfig.setZkConfig(zkConfig);
			// New session factory,强烈建议使用单例
			MessageSessionFactory sessionFactory = new MetaMessageSessionFactory(metaClientConfig);
			// create consumer,强烈建议使用单例

			// 生成处理线程
			ConsumerConfig cc = new ConsumerConfig(group);
			HiveConf hc = new HiveConf();
			if (hc.getBoolVar(ConfVars.NEWMS_IS_GET_ALL_OBJECTS) ||
			    hc.getBoolVar(ConfVars.NEWMS_CONSUME_FROM_MAX_OFFSET)) {
        cc.setConsumeFromMaxOffset();
      }
			MessageConsumer consumer = sessionFactory.createConsumer(cc);

			// 订阅事件，MessageListener是事件处理接口
			consumer.subscribe(topic, 1024, new MessageListener() {

				@Override
				public Executor getExecutor() {
					return null;
				}

				@Override
				public void recieveMessages(final Message message) {
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
					String data = new String(message.getData());
					if (DiskManager.role != null) {
					  switch (DiskManager.role) {
					  case MASTER:
					    LOG.info("Consume msg from metaq's topic=" + topic + ": " + data);
					    break;
					  case SLAVE:
					  default:
					    LOG.info("Slave ignore consume msg from metaq's topic=" + topic + ": " + data);
					    return;
					  }
					}
					int time = 0;
					DDLMsg msg = DDLMsg.fromJson(data);
					// FIXME: if we are in IS_OLD_WITH_NEW mode, we can ignore file-related ops
					if (conf.getBoolVar(ConfVars.NEWMS_IS_OLD_WITH_NEW) && (
					    msg.getEvent_id() == MSGType.MSG_CREATE_FILE ||
					    msg.getEvent_id() == MSGType.MSG_DEL_FILE ||
					    msg.getEvent_id() == MSGType.MSG_REP_FILE_CHANGE ||
					    msg.getEvent_id() == MSGType.MSG_REP_FILE_ONOFF ||
					    msg.getEvent_id() == MSGType.MSG_STA_FILE_CHANGE ||
					    msg.getEvent_id() == MSGType.MSG_FILE_USER_SET_REP_CHANGE ||
					    msg.getEvent_id() == MSGType.MSG_FAIL_NODE ||
					    msg.getEvent_id() == MSGType.MSG_BACK_NODE ||
					    msg.getEvent_id() == MSGType.MSG_REFRESH_ALL
					    // BUG-XXX: if we ignore CREATE_DEVICE and DEL_DEVICE, we can NOT
					    // handle online/offline device
					    //msg.getEvent_id() == MSGType.MSG_CREATE_DEVICE ||
					    //msg.getEvent_id() == MSGType.MSG_DEL_DEVICE
					    )) {
					  // Resend by NewMS from OldMS to meta-test topic
					  MsgServer.pdSend(msg);
					  return;
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

			});
			consumer.completeSubscribe();
		}

	}

	public static DDLMsg generateDDLMsg(long event_id,long db_id,long node_id ,PersistenceManager pm , Object eventObject,HashMap<String,Object> old_object_params){
    Long id = -1l;
    long now = new Date().getTime()/1000;
    return new MSGFactory.DDLMsg(event_id, id, old_object_params, eventObject, max_msg_id++, db_id, node_id, now, null,old_object_params);
  }

	// FIXME: BUG-XXX: we have to consider metaDB failed, and cache localqueue messages!
	public static class LocalConsumer implements Runnable {
		private final Semaphore lcsem  = new Semaphore(0);
		private ObjectStore ob;

		public LocalConsumer() {
			ob = new ObjectStore();
    	ob.setConf(new HiveConf());
		}

		public void release()	{
			lcsem.release();
		}

		@Override
		public void run() {
			while (true) {
			  try {
          try {
						lcsem.acquire();
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
          DDLMsg msg = localQueue.poll();
          if (msg == null) {
              continue;
          }
          LOG.info("consume msg from localq: " + msg.toJson());
          if (msg.getEventObject() == null) {
          	LOG.warn("eventObject is null, event id is " + msg.getEvent_id());
          	continue;
          }
          int event_id = (int) msg.getEvent_id();
          switch (event_id) {
	          case MSGType.MSG_REP_FILE_CHANGE:
	          {
	          	String op = msg.getMsg_data().get("op").toString();
	          	SFileLocation sfl = (SFileLocation) msg.getEventObject();
	          	if (op.equals("add")) {
	          		try {
									ob.createFileLocation(sfl);
								} catch (Exception e) {
									LOG.error(e,e);
									LOG.error("handle msg failed(resend by newms): " + msg.toJson());
									msg.getOld_object_params().put("__type", "newms");
									MsgServer.pdSend(msg);
								}
	          	}
	          	if (op.equals("del")) {
	          		try {
									ob.delSFileLocation(sfl.getDevid(), sfl.getLocation());
								} catch (MetaException e) {
									LOG.error(e,e);
									LOG.error("handle msg failed(resend by newms): " + msg.toJson());
									msg.getOld_object_params().put("__type", "newms");
									MsgServer.pdSend(msg);
								}
	          	}
	          	break;
	          }
	          case MSGType.MSG_FILE_USER_SET_REP_CHANGE:
	          case MSGType.MSG_STA_FILE_CHANGE:
	          {
	          	SFile sf = (SFile) msg.getEventObject();
	          	try {
								ob.updateSFile(sf);
							} catch (MetaException e) {
								LOG.error(e,e);
								LOG.error("handle msg failed(resend by newms): " + msg.toJson());
								msg.getOld_object_params().put("__type", "newms");
								MsgServer.pdSend(msg);
							}

	          	break;
	          }
	          case MSGType.MSG_REP_FILE_ONOFF:
	          {
	          	SFileLocation sfl = (SFileLocation) msg.getEventObject();
	          	try {
								ob.updateSFileLocation(sfl);
							} catch (MetaException e) {
								LOG.error(e,e);
								LOG.error("handle msg failed(resend by newms): " + msg.toJson());
								msg.getOld_object_params().put("__type", "newms");
								MsgServer.pdSend(msg);
							}
	          	break;
	          }
	          case MSGType.MSG_CREATE_FILE:
	          {
	          	try {
								SFile sf = (SFile) msg.getEventObject();
								ob.persistFile(sf);
							} catch (Exception e) {
								LOG.error(e,e);
								LOG.error("handle msg failed(resend by newms): " + msg.toJson());
								msg.getOld_object_params().put("__type", "newms");
								MsgServer.pdSend(msg);
							}
	          	break;
	          }
	          case MSGType.MSG_DEL_FILE:
	          {
	          	try {
	          		SFile sf = (SFile) msg.getEventObject();
	          		if (sf.getLocations() != null) {
                  for (SFileLocation sfl : sf.getLocations()) {
                    ob.delSFileLocation(sfl.getDevid(), sfl.getLocation());
                  }
                }
								ob.delSFile(sf.getFid());
							} catch (MetaException e) {
								LOG.error(e,e);
								LOG.error("handle msg failed(resend by newms): " + msg.toJson());
								msg.getOld_object_params().put("__type", "newms");
								MsgServer.pdSend(msg);
							}
	          	break;
						}
	          case MSGType.MSG_FAIL_NODE:
	          case MSGType.MSG_BACK_NODE:
	          {
	          	Node n = (Node) msg.getEventObject();
	          	try {
								ob.updateNode(n);
							} catch (MetaException e) {
								LOG.error(e,e);
								LOG.error("handle msg failed(resend by newms): " + msg.toJson());
								msg.getOld_object_params().put("__type", "newms");
								MsgServer.pdSend(msg);
							}
	          	break;
	          }
	          case MSGType.MSG_CREATE_DEVICE:
	          {
	          	Device d = (Device) msg.getEventObject();
	          	try {
	          		ob.createDevice(d);
	          	} catch (MetaException e){
	          		LOG.error(e,e);
	          		LOG.error("handle msg failed(resend by newms): " + msg.toJson());
	          		msg.getOld_object_params().put("__type", "newms");
	          		MsgServer.pdSend(msg);
	          	}
	          	break;
	          }
	          case MSGType.MSG_DEL_DEVICE:
	          {
	          	String devid = msg.getMsg_data().get("devid").toString();
	          	try {
	          		ob.delDevice(devid);
	          	} catch (MetaException e){
	          		LOG.error(e,e);
								LOG.error("handle msg failed(resend by newms): " + msg.toJson());
								msg.getOld_object_params().put("__type", "newms");
								MsgServer.pdSend(msg);
	          	}
	          	break;
	          }
	        }//end of switch
			  }  catch (Exception e) {
			    try {
			      ob = new ObjectStore();
			      ob.setConf(new HiveConf());
			    } catch (Exception e1) {
			      LOG.error(e1, e1);
			      LOG.error("Exception in exception handling, BAD!");
			    }
			  }
			}
		}

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}
}
