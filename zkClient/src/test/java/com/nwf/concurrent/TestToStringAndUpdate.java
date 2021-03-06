package com.nwf.concurrent;

import com.vmlens.api.AllInterleavings;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

/**
 * @author niewenfeng
 * @className TestToStringAndUpdate
 * @description todo
 * @date 2020/12/15 17:37
 **/
public class TestToStringAndUpdate {
    @Test
    public void testMutableAddress() throws InterruptedException {
        try (AllInterleavings allInterleavings = new AllInterleavings("TestToStringAndUpdate_Not_Thread_Safe");) {
            while (allInterleavings.hasNext()) {
                MutableAddress address = new MutableAddress("E. Bonanza St.", "South Park", "456 77 99");
                String readAddress = null;
                Thread first = new Thread(() -> {
                    address.update("Evergreen Terrace", "Springfield");
                });
                first.start();
                readAddress = address.toString();
                first.join();

                assertTrue("readAddress:" + readAddress,
                        readAddress.equals("street=E. Bonanza St.,city=South Park,phoneNumber=456 77 99") || readAddress
                                .equals("street=Evergreen Terrace,city=Springfield,phoneNumber=456 77 99"));

            }
        }
    }

    @Test
    public void testAddressUsingCopyOnWrite() throws InterruptedException {
        try (AllInterleavings allInterleavings = new AllInterleavings("TestToStringAndUpdate_Thread_Safe");) {
            while (allInterleavings.hasNext()) {
                AddressUsingCopyOnWrite address = new AddressUsingCopyOnWrite("E. Bonanza St.", "South Park", "456 77 99");
                String readAddress = null;
                Thread first = new Thread(() -> {
                    address.update("Evergreen Terrace", "Springfield");
                });
                first.start();
                readAddress = address.toString();
                first.join();
                assertTrue("readAddress:" + readAddress,
                        readAddress.equals("street=E. Bonanza St.,city=South Park,phoneNumber=456 77 99") || readAddress
                                .equals("street=Evergreen Terrace,city=Springfield,phoneNumber=456 77 99"));

            }
        }
    }

    @Test
    public void testAddressNonVolatile() throws InterruptedException {
        try (AllInterleavings allInterleavings = new AllInterleavings("TestToStringAndUpdate_Data_Race");) {
            while (allInterleavings.hasNext()) {
                AddressNonVolatile address = new AddressNonVolatile("E. Bonanza St.", "South Park", "456 77 99");
                String readAddress = null;
                Thread first = new Thread(() -> {
                    address.update("Evergreen Terrace", "Springfield");
                });
                first.start();
                readAddress = address.toString();
                first.join();
                assertTrue("readAddress:" + readAddress,
                        readAddress.equals("street=E. Bonanza St.,city=South Park,phoneNumber=456 77 99") || readAddress
                                .equals("street=Evergreen Terrace,city=Springfield,phoneNumber=456 77 99"));

            }
        }
    }
}
