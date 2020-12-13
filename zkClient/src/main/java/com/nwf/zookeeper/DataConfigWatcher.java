package com.nwf.zookeeper;

import com.google.common.base.Joiner;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DataConfigWatcher implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(DataConfigWatcher.class);

    AtomicInteger seq = new AtomicInteger();
    /**
     * 定义session失效时间
     */
    private static final int SESSION_TIMEOUT = 10000;
    /**
     * zookeeper服务器地址
     */
    private static final String ZK_ADDR = "127.0.0.1:2181";

    private static final String ZK_PATH = "/config/data";

    private static final String LOG_PREFIX_OF_MAIN = "【Main】";

    /**
     * zk变量
     */
    private ZooKeeper zk = null;


    private Map<String, String> dataMap;
    private Set<String> pathSet;

    public DataConfigWatcher() {
        pathSet = new HashSet<>();
        dataMap = new HashMap<>();
        this.createConnection(ZK_ADDR, SESSION_TIMEOUT);
        readChildrenData(ZK_PATH, true);
    }

    private void readChildrenData(String parentPath, boolean needWatch) {
        readData(parentPath, needWatch);
        List<String> subPaths = getChildren(parentPath, true);
        if (null != subPaths) {
            subPaths.forEach(subPath -> {
                String nodePath = String.format("%s/%s", parentPath, subPath);
                if (!pathSet.contains(nodePath)) {
                    pathSet.add(nodePath);
                    readData(nodePath, true);
                }
            });
        }
    }


    /**
     * 创建ZK连接
     *
     * @param connectAddr    ZK服务器地址列表
     * @param sessionTimeout Session超时时间
     */
    public void createConnection(String connectAddr, int sessionTimeout) {
//        this.releaseConnection();
        try {
            //this表示把当前对象进行传递到其中去（也就是在主函数里实例化的new ZooKeeperWatcher()实例对象）
            zk = new ZooKeeper(connectAddr, sessionTimeout, this);
            System.out.println(LOG_PREFIX_OF_MAIN + "开始连接ZK服务器");
//            connectedSemaphore.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭ZK连接
     */
    public void releaseConnection() {
        if (this.zk != null) {
            try {
                this.zk.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void outputMap() {
        List<String> strList = new ArrayList<>();
        dataMap.forEach((key,value)->{
            strList.add(String.format("%s:%s", key, value));
        });
        logger.info("dataMap:{}", Joiner.on("|").join(strList));
        System.out.println("dataMap:" + Joiner.on("|").join(strList));
    }
    /**
     * 读取指定节点数据内容
     *
     * @param path 节点路径
     * @return
     */
    public void readData(String path, boolean needWatch) {
        try {
            System.out.println("读取数据操作...");
            String value = new String(this.zk.getData(path, needWatch, null));
            dataMap.put(path, value);
        } catch (Exception ex) {
            logger.error("read data error, Exception:", ex);
        }
    }

    /**
     * 判断指定节点是否存在
     *
     * @param path 节点路径
     */
    public Stat exists(String path, boolean needWatch) {
        try {
            return this.zk.exists(path, needWatch);
        } catch (Exception ex) {
            logger.error("exists path error. Exception:", ex);
            return null;
        }
    }

    /**
     * 获取子节点
     *
     * @param path 节点路径
     */
    private List<String> getChildren(String path, boolean needWatch) {
        try {
            System.out.println("读取子节点操作...");
            return this.zk.getChildren(path, needWatch);
        } catch (Exception ex) {
            logger.error("getChildren error. Exception:", ex);
            return null;
        }
    }

    @Override
    public void process(WatchedEvent event) {
//        System.out.println("进入 process 。。。。。event = " + event);
        logger.info("进入process... event = {}", event);

        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            logger.error("InterruptedException:", e);
        }

        if (event == null) {
            return;
        }

        // 连接状态
        Event.KeeperState keeperState = event.getState();
        // 事件类型
        Event.EventType eventType = event.getType();
        // 受影响的path
        String path = event.getPath();
        //原子对象seq 记录进入process的次数
        String logPrefix = "【Watcher-" + this.seq.incrementAndGet() + "】";

        logger.info("{} 收到Watcher通知", logPrefix);
        logger.info("{}连接状态:\t{}", logPrefix, keeperState.toString());
        logger.info("{}事件类型:\t{}", logPrefix, eventType.toString());

        if (Event.KeeperState.SyncConnected == keeperState) {
            // 成功连接上ZK服务器
            if (Event.EventType.None == eventType) {
                logger.info("{} 成功连接上ZK服务器", logPrefix);
//                connectedSemaphore.countDown();
            }
            //创建节点
            else if (Event.EventType.NodeCreated == eventType) {
                logger.info("{} 节点创建", logPrefix);
                readData(event.getPath(), true);
                outputMap();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //更新节点
            else if (Event.EventType.NodeDataChanged == eventType) {
                System.out.println(logPrefix + "节点数据更新");
                logger.info("{} 节点数据更新", logPrefix);
                readData(event.getPath(), true);
                outputMap();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //更新子节点
            else if (Event.EventType.NodeChildrenChanged == eventType) {
                System.out.println(logPrefix + "子节点变更");
                logger.info("{} 子节点变更", logPrefix);
                readChildrenData(event.getPath(), true);
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //删除节点
            else if (Event.EventType.NodeDeleted == eventType) {
                System.out.println(logPrefix + "节点 " + path + " 被删除");
                logger.info("{} 节点 {} 被删除", logPrefix, path);
                dataMap.remove(event.getPath());
                outputMap();
            } else ;
        } else if (Event.KeeperState.Disconnected == keeperState) {
            System.out.println(logPrefix + "与ZK服务器断开连接");
            logger.info("{} 与ZK服务器断开连接", logPrefix);
        } else if (Event.KeeperState.AuthFailed == keeperState) {
            System.out.println(logPrefix + "权限检查失败");
            logger.info("{} 权限检查失败", logPrefix);
        } else if (Event.KeeperState.Expired == keeperState) {
            System.out.println(logPrefix + "会话失效");
            logger.info("{} 会话失效", logPrefix);
        } else ;

        System.out.println("--------------------------------------------");
    }

    public static void main(String[] args) throws Exception {
        DataConfigWatcher dataConfig = new DataConfigWatcher();
        Thread.sleep(1000000);
        System.out.println("end");
    }
}
