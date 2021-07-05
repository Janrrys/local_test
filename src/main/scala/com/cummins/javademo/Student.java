package com.cummins.javademo;

import java.util.*;

/**
 * Created by newforesee on 2018/7/13.
 */
class Student {
    private String name;
    private int age;
    private int grade;

    //构造方法
    public Student(String name, int age, int grade) {
        this.name = name;
        this.age = age;
        this.grade = grade;
    }

    //getter and setter
    public int getGrade() {
        return grade;
    }

    public void setGrade(int grade) {
        this.grade = grade;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    //格式化学生列表,去重
    public static void formatList(ArrayList<Student> studentArrayList) {

        ArrayList<Student> list1 = new ArrayList<>();
        for (Student student : studentArrayList) {
            if (!(list1.contains(student))) {
                list1.add(student);
            }
        }
        //利用比较器排序
        Collections.sort(list1, new ComWhithAge());
        //利用址传递更改原list的内容
        studentArrayList.clear();
        studentArrayList.addAll(list1);
    }

    //遍历学生
    public static void traverseMapByKeySet(Map<String, ArrayList<Student>> map) {
        System.out.println("From traverseMapByKeySet");
        Set<String> set = map.keySet();
        for (String key : set) {
            ArrayList<Student> students = map.get(key);
            display(students);
        }

    }

    public static void traverseMapByEntry(Map<String, ArrayList<Student>> map) {
        System.out.println("Frome traverseMapByEntry");
        //1.先得到存着entry的Set
        Set<Map.Entry<String, ArrayList<Student>>> set = map.entrySet();
        //2.得到迭代器
        //3.遍历
        for (Map.Entry<String, ArrayList<Student>> next : set) {
            ArrayList<Student> students = next.getValue();
            display(students);
        }
        //等效于
        /*while (iterator1.hasNext()) {
            Map.Entry<String, ArrayList<Student>> next = iterator1.next();
            ArrayList<Student> students = next.getValue();
            display(students);
        }*/

    }

    private static void display(ArrayList<Student> students) {
        for (Student student : students) {

            System.out.println(
                    "班级:" + student.getGrade() + "\t" +
                            "姓名:" + student.getName() + " " +
                            "年龄:" + student.getAge());
        }
    }

    @Override
    public String toString() {
        return "Student{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", grade=" + grade +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Student student = (Student) o;

        return age == student.age && (name != null ? name.equals(student.name) : student.name == null);
        /*
        if (age == student.age)
            if (name != null ? name.equals(student.name) : student.name == null) return true;
        return false;
        */

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + age;
        return result;
    }
}