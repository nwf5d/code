package com.nwf.concurrent;

/**
 * @author niewenfeng
 * @className AddressNonVolatile
 * @description todo
 * @date 2020/12/15 17:32
 **/
public class AddressNonVolatile {
    private AddressValue addressValue;
    private final Object LOCK = new Object();
    @Override
    public String toString() {
        AddressValue local = addressValue;
        return "street=" + local.getStreet() + ",city=" + local.getCity() + ",phoneNumber=" + local.getPhoneNumber();
    }
    public AddressNonVolatile(String street, String city, String phone) {
        this.addressValue = new AddressValue( street,  city,  phone);
    }
    public void update(String street ,String city ) {
        synchronized(LOCK){
            addressValue = new AddressValue(  street,  city,  addressValue.getPhoneNumber() );
        }
    }
}
