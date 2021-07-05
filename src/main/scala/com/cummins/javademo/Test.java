package com.cummins.javademo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by newforesee on 2018/7/13.
 *
 */
public class Test {
    public static void main(String[] args) {



        Map<String, ArrayList<Student>> map = new HashMap<>();
        ArrayList<Student> studentsOfGrade1 = new ArrayList<>();
        studentsOfGrade1.add(new Student("张三", 22, 1));
        studentsOfGrade1.add(new Student("李四", 23, 1));
        studentsOfGrade1.add(new Student("王五", 33, 1));
        studentsOfGrade1.add(new Student("赵六", 20, 1));
        studentsOfGrade1.add(new Student("赵六", 20, 1));
        Student.formatList(studentsOfGrade1);
        ArrayList<Student> studentsOfGrade2 = new ArrayList<>();
        studentsOfGrade2.add(new Student("赵钱孙", 25, 2));
        studentsOfGrade2.add(new Student("李周吴", 20, 2));
        studentsOfGrade2.add(new Student("郑王", 20, 2));
        studentsOfGrade2.add(new Student("冯陈褚", 21, 2));
        studentsOfGrade2.add(new Student("卫蒋沈", 19, 2));
        Student.formatList(studentsOfGrade2);


        map.put("一班", studentsOfGrade1);
        map.put("二班", studentsOfGrade2);
        Student.traverseMapByEntry(map);
        Student.traverseMapByKeySet(map);


    }


}






