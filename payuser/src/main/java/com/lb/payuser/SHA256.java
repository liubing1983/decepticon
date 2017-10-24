package com.lb.payuser;

import java.security.MessageDigest;

/**
 * Created by samsung on 2017/4/21.
 */
public class SHA256 {

    public static String testSha256(String t) {
        String s = "";
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(t.getBytes("GBK"));
            // for (byte b : md.digest()){
            //    System.out.format("%02x", b);
            // }
            s = bytes2Hex(md.digest()); // to HexString

        } catch (Exception e) {
            e.printStackTrace();
        }
        return s;
    }

    public static String bytes2Hex(byte[] bts) {
        String des = "";
        String tmp = null;
        for (int i = 0; i < bts.length; i++) {
            tmp = (Integer.toHexString(bts[i] & 0xFF));
            if (tmp.length() == 1) {
                des += "0";
            }
            des += tmp;
        }
        return des;
    }

    public static void main(String[] args) {
        System.out.println(testSha256("123") + "----");
    }

}
