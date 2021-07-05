package com.cummins.javademo;

import java.util.Comparator;

/**
 * Created by newforesee on 2018/7/13.
 *
 */
//构建比较器
class ComWhithAge implements Comparator<Student> {

    @Override
    public int compare(Student o1, Student o2) {
        int num = o1.getAge() - o2.getAge();
        return num == 0 ? o2.getName().compareTo(o1.getName()) : num;
    }
}
