package com.nwf.zookeeper;

import org.apache.log4j.BasicConfigurator;

/**
 * @author niewenfeng
 * @className Main
 * @description todo
 * @date 2020/12/15 17:55
 **/
public class Main {
    public static void main(String[] args) throws Exception {
//        DataConfigWatcher dataConfig = DataConfigWatcher.getInstance();
        BasicConfigurator.configure();
        ConfigData dataConfig = ConfigData.getInstance();
        for (int i=0;i<100;i++) {
            Thread.sleep(1000);
            dataConfig.outputMap();
        }
        System.out.println("end");
    }
}
