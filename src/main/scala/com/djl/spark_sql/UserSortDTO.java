package com.djl.spark_sql;

import java.io.Serializable;

/**
 * 用户排序的对象
 */
public class UserSortDTO implements Comparable<UserSortDTO>, Serializable {
    private Long id;
    private String name;
    private Integer age;
    private Integer fv;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Integer getFv() {
        return fv;
    }

    public void setFv(Integer fv) {
        this.fv = fv;
    }

    public UserSortDTO(Long id, String name, Integer age, Integer fv) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.fv = fv;
    }

    @Override
    public int compareTo(UserSortDTO o) {
        if (this.getFv() == o.getFv()) {
            if (this.getAge() == o.getAge()) {
                return 0;
            } else {
                return this.getAge() - o.getAge() > 0 ? -1 : 1;
            }
        } else if (this.getFv() > o.getFv()) {
            return 1;
        } else {
            return -1;
        }
    }
}
