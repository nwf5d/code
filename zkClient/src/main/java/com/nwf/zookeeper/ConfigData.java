package com.nwf.zookeeper;

import com.google.common.base.Joiner;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author niewenfeng
 * @className ConfigData
 * @description todo
 * @date 2020/12/14 9:51
 **/
public class ConfigData extends SyncZkData {
    private static Logger logger = LoggerFactory.getLogger(ConfigData.class);
    private static final String ZK_ADDR = "127.0.0.1:2181";
    private static final String ZK_PATH = "/config/data";
    private int SESSION_TIMEOUT = 10000;

    private static ConfigData instance;
    private Map<String, String> dataMap;

    public static ConfigData getInstance(){
        if (null == instance) {
            instance = new ConfigData();
        }
        return instance;
    }

    public void outputMap() {
        List<String> strList = new ArrayList<>();
        dataMap.forEach((key,value)->{
            strList.add(String.format("%s:%s", key, value));
        });
//        logger.info("dataMap:{}, thread:{}", Joiner.on("|").join(strList), Thread.currentThread().toString());
        logger.info("dataMap:{}", Joiner.on("|").join(strList));
//        System.out.println("dataMap:" + Joiner.on("|").join(strList));
    }

    private ConfigData() {
        super();
        dataMap = new ConcurrentHashMap<>();
        createConnection(ZK_ADDR, SESSION_TIMEOUT);
        readChildrenData(ZK_PATH, true);
    }

    @Override
    public void readData(String path, boolean needWatch) {
        try {
            String data = new String(this.zk.getData(path, needWatch, null));
            dataMap.put(path, data);
        } catch (KeeperException ex) {
            logger.error("read data from zknode, keepException: ", ex);
        } catch (InterruptedException ex) {
            logger.error("read data from zknode, Exception: ", ex);
        }
    }

    @Override
    public void removeData(String zkPath, boolean needWatch) {
        if (!dataMap.containsKey(zkPath)) {
            logger.warn("zkPath={} not found in map data.", zkPath);
            return;
        }

        dataMap.remove(zkPath);
    }
}
