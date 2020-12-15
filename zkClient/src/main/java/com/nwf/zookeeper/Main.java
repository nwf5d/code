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

        Thread first = new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                try {
                    Thread.sleep(1000);
                    dataConfig.outputMap();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
//            address.update("Evergreen Terrace", "Springfield");
        });
        first.start();
//        readAddress = address.toString();


        for (int i=0;i<100;i++) {
            Thread.sleep(1000);
            dataConfig.outputMap();
        }

        first.join();
        System.out.println("end");
    }
}
