package com.yuxin.kvlsmtree.service;

import java.io.Closeable;

public interface KVStore extends Closeable {


    void set(String key, String value);
    String get(String key);

    void rm(String key);

    void printfStats();
}
