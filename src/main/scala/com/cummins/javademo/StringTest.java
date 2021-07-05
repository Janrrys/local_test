package com.cummins.javademo;

public class StringTest {
    public static void main(String[] args) {
        String str = new String("188 * 22");

//        String[] s = str.split("\\*",1);
//        for (String s1 : s) {
//            System.out.println(s1);
//        }

        String replace = str.replaceAll("\\*", " ");
        System.out.println(replace.substring(0,5));
    }
}
