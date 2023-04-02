package com.yuxin.kvlsmtree.model.sstable;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import com.yuxin.kvlsmtree.model.Position;
import com.yuxin.kvlsmtree.model.command.Command;
import com.yuxin.kvlsmtree.utils.FileUtil;
import com.yuxin.kvlsmtree.utils.LoggerUtil;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class SsTable {
    private final Logger LOGGER = LoggerFactory.getLogger(SsTable.class);

    public static final String RW_MODE = "rw";

    private TableMetaInfo tableMetaInfo;

    private Integer level;

    private TreeMap<String, Position> sparseIndex;

    private final RandomAccessFile tableFile;

    private final String filePath;

    private boolean enablePartDataCompress;

    private SsTable(String filePath, boolean enablePartDataCompress) {
        this.filePath = filePath;
        this.enablePartDataCompress = enablePartDataCompress;
        try {
            this.tableFile = new RandomAccessFile(filePath, RW_MODE);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private SsTable(Long fileNumber, int partSize, boolean enablePartDataCompress, Integer level) {
        this.tableMetaInfo = new TableMetaInfo();
        this.tableMetaInfo.setNumber(fileNumber);
        this.tableMetaInfo.setPartSize(partSize);
        this.level = level;
        this.filePath = FileUtil.buildSsTableFilePath(fileNumber, level);
        this.enablePartDataCompress = enablePartDataCompress;
        try {
            this.tableFile = new RandomAccessFile(filePath, RW_MODE);
            tableFile.seek(0);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
        this.sparseIndex = new TreeMap<>();
    }

    public static SsTable createFromIndex(Long fileNumber, int parSize,
                                          ConcurrentSkipListMap<String, Command> index,
                                          boolean enablePartDataCompress,
                                          Integer level) throws IOException {
        SsTable ssTable = new SsTable(fileNumber, parSize, enablePartDataCompress, level);
        ssTable.initFromIndex(index);
        return ssTable;
    }

    public static SsTable createFromFile(String filePath, boolean enablePartDataCompress) {
        SsTable ssTable = new SsTable(filePath, enablePartDataCompress);
        ssTable.restoreFromFile();
        return ssTable;
    }

    public Command query(String key) {
        try {
            // 这里
        } catch (Throwable t) {
            t.printStackTrace();
            throw new RuntimeException(t);
        }
    }
    private void restoreFromFile() {
        try {
            TableMetaInfo tableMetaInfo = TableMetaInfo.readFromFile(tableFile);
            LoggerUtil.debug((org.slf4j.Logger) LOGGER, "[SsTable][restoreFromFile][tableMetaInfo]: {}", tableMetaInfo);
            byte[] indexBytes = new byte[(int) tableMetaInfo.getIndexLen()];
            tableFile.seek(tableMetaInfo.getIndexStart());
            tableFile.read(indexBytes);
            String indexStr = new String(indexBytes, StandardCharsets.UTF_8);
            LoggerUtil.debug((org.slf4j.Logger) LOGGER, "[SsTable][restoreFromFile][indexStr]: {}", indexStr);
            this.sparseIndex = JSONObject.parseObject(indexStr, new TypeReference<TreeMap<String, Position>>());
            // 这里
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void initFromIndex(ConcurrentSkipListMap<String, Command> index) {
    }


}
