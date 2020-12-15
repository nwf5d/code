package com.nwf.concurrent;

/**
 * @author niewenfeng
 * @className AddressUsingCopyOnWrite
 * @description todo
 * @date 2020/12/15 17:32
 **/
public class AddressUsingCopyOnWrite {
    private volatile AddressValue addressValue;
    private final Object LOCK = new Object();
    public AddressUsingCopyOnWrite(String street,
                                   String city, String phone) {
        this.addressValue = new AddressValue( street,
                city,  phone);
    }
    public void update(String street ,String city ) {
        synchronized(LOCK){
            addressValue = new AddressValue(  street,  city,
                    addressValue.getPhoneNumber() );
        }
    }
    public String toString() {
        AddressValue local = addressValue;
        return "street=" + local.getStreet()
                + ",city=" + 	local.getCity() +
                ",phoneNumber=" + local.getPhoneNumber();
    }
}
