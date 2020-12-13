package com.nwf.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class BasicWatcher implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(BasicWatcher.class);

    AtomicInteger seq = new AtomicInteger();

    private int SESSION_TIMEOUT = 10000;
    /**
     * zookeeper服务器地址
     */
    private String ZK_ADDR = "127.0.0.1:2181";

    private String ZK_PATH = "/config/data";

    private ZooKeeper zk = null;


    private Set<String> pathSet;

    public BasicWatcher() {
        this.pathSet = new HashSet<>();
    }

    public BasicWatcher(String ZK_ADDR, String ZK_PATH) {
        this.ZK_ADDR = ZK_ADDR;
        this.ZK_PATH = ZK_PATH;
        this.pathSet = new HashSet<>();
        this.createConnection(this.ZK_ADDR, SESSION_TIMEOUT);
        readChildrenData(this.ZK_PATH, true);
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
            System.out.println("开始连接ZK服务器");
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
     * 判断指定节点是否存在
     *
     * @param path 节点路径
     */
    public Stat exists(String path, boolean needWatch) {
        try {
            return this.zk.exists(path, needWatch);
        } catch (Exception ex) {
//            logger.error("exists path error. Exception:", ex);
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
//            logger.error("getChildren error. Exception:", ex);
            return null;
        }
    }

    public abstract void readData(String path, boolean needWatch);
    public abstract void removeData(String path, boolean needWatch);

    @Override
    public void process(WatchedEvent event) {
//        System.out.println("进入 process 。。。。。event = " + event);
        logger.info("进入process... event = {}", event);
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

        switch (keeperState) {
            case SyncConnected:
                processEvent(event);
                break;
            case AuthFailed:
                System.out.println(logPrefix + "权限检查失败");
                logger.info("{} 权限检查失败", logPrefix);
                break;
            case Expired:
                System.out.println(logPrefix + "会话失效");
                logger.info("{} 会话失效", logPrefix);
            default:
                logger.info("{} default status.", keeperState.toString());
        }

        System.out.println("--------------------------------------------");
    }

    private void processEvent(WatchedEvent event) {
        String logPrefix = "【Watcher-" + this.seq.get() + "】";
        Event.EventType eventType = event.getType();
        switch (eventType) {
            case None:
                logger.info("{} 成功连接上ZK服务器", logPrefix);
                break;
            case NodeCreated:
                logger.info("{} 节点创建", logPrefix);
                readData(event.getPath(), true);
                break;
            case NodeDataChanged:
                logger.info("{} 节点数据更新", logPrefix);
                readData(event.getPath(), true);
                break;
            case NodeChildrenChanged:
                logger.info("{} 子节点变更", logPrefix);
                readChildrenData(event.getPath(), true);
                break;
            case NodeDeleted:
                logger.info("{} 节点 {} 被删除", logPrefix, event.getPath());
                removeData(event.getPath(), false);
                break;
            default:
                logger.info("default event, not processed!");
        }
    }
}
