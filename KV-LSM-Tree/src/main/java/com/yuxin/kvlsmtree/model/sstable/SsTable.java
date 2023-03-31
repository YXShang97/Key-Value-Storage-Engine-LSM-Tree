package com.yuxin.kvlsmtree.model.sstable;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import com.yuxin.kvlsmtree.model.Position;

import java.util.TreeMap;

public class SsTable {
    private final Logger LOGGER = LoggerFactory.getLogger(SsTable.class);

    public static final String RW_MODE = "rw";

    private TableMetaInfo tableMetaInfo;

    private Integer level;

    private TreeMap<String, Position> sparseIndex;
}
