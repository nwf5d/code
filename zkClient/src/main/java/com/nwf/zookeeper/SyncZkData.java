package com.nwf.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author niewenfeng
 * @className SyncZkData
 * @description todo
 * @date 2020/12/14 9:54
 **/
public abstract class SyncZkData implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(SyncZkData.class);
    protected ZooKeeper zk = null;

    private int SESSION_TIMEOUT = 10000;
    /**
     * zookeeper服务器地址
     */
    private String ZK_ADDR = "127.0.0.1:2181";

    private String ZK_PATH = "/config/data";

    private Set<String> pathSet;

    public SyncZkData(String ZK_ADDR, String ZK_PATH) {
        this.ZK_ADDR = ZK_ADDR;
        this.ZK_PATH = ZK_PATH;
        this.pathSet = new HashSet<>();
        this.createConnection(this.ZK_ADDR, SESSION_TIMEOUT);
//        readChildrenData(this.ZK_PATH, true);
    }

    /**
     * 创建ZK连接
     *
     * @param connectAddr    ZK服务器地址列表
     * @param sessionTimeout Session超时时间
     */
    public void createConnection(String connectAddr, int sessionTimeout) {
        this.releaseConnection();
        try {
            //this表示把当前对象进行传递到其中去（也就是在主函数里实例化的new ZooKeeperWatcher()实例对象）
            zk = new ZooKeeper(connectAddr, sessionTimeout, this);
            logger.info("begin to connect zk server.");
        } catch (Exception ex) {
            logger.error("connect zk server error! Exception:", ex);
//            System.out.println("connect zk server error!");
        }
    }

    /**
     * 关闭ZK连接
     */
    public void releaseConnection() {
        if (this.zk != null) {
            try {
                this.zk.close();
            } catch (InterruptedException ex) {
                logger.error("release connection error! Exception:", ex);
            }
        }
    }

    protected void readChildrenData(String parentPath, boolean needWatch) {
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
            logger.error("check zk path={} exists error! Exception:", path, ex);
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
            logger.info("read child node");
            return this.zk.getChildren(path, needWatch);
        } catch (Exception ex) {
            logger.error("getChildren error. Exception:", ex);
        }
        return Collections.emptyList();
    }

    protected abstract void readData(String path, boolean needWatch);
    protected abstract void removeData(String path, boolean needWatch);

    @Override
    public void process(WatchedEvent event) {
        if (event == null) {
            logger.warn("process null event.");
            return;
        }
        logger.info("begin process event = {}", event);

        // 连接状态
        Event.KeeperState keeperState = event.getState();
        // 事件类型
        Event.EventType eventType = event.getType();

        logger.info("receive Watcher event. keeperStatus:{}, eventType:{}", keeperState.toString(),
                eventType.toString());

        switch (keeperState) {
            case SyncConnected:
                processEvent(event);
                break;
            case AuthFailed:
                logger.error("zk client AuthFailed.");
                break;
            case Expired:
                logger.warn("zk client Session expired.");
                break;
            default:
                logger.info("{} default status.", keeperState.toString());
        }
    }

    private void processEvent(WatchedEvent event) {
        Event.EventType eventType = event.getType();
        switch (eventType) {
            case None:
                logger.info("connect zk server success !");
                break;
            case NodeCreated:
                logger.info("zk node created.");
                readData(event.getPath(), true);
                break;
            case NodeDataChanged:
                logger.info("node data updated.");
                readData(event.getPath(), true);
                break;
            case NodeChildrenChanged:
                logger.info("children data changed.");
                readChildrenData(event.getPath(), true);
                break;
            case NodeDeleted:
                logger.info("zk node [{}] was deleted.", event.getPath());
                removeData(event.getPath(), false);
                break;
            default:
                logger.info("default event, not processed!");
        }
    }
}
