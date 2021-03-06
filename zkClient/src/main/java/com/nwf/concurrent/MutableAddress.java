package com.nwf.concurrent;

/**
 * @author niewenfeng
 * @className MutableAddress
 * @description todo
 * @date 2020/12/15 17:31
 **/
public class MutableAddress {
    private volatile String street;
    private volatile String city;
    private volatile String phoneNumber;
    public MutableAddress(String street, String city, String phoneNumber) {
        this.street = street;
        this.city = city;
        this.phoneNumber = phoneNumber;
    }
    public void update(String street ,String city ) {
        this.street = street;
        this.city = city;
    }
    public String toString() {
        return "street=" + street + ",city=" + city +
                ",phoneNumber=" + phoneNumber;
    }
}
