package com.yuxin.kvlsmtree.model.sstable;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import com.yuxin.kvlsmtree.model.Position;
import com.yuxin.kvlsmtree.model.command.Command;
import com.yuxin.kvlsmtree.model.command.RmCommand;
import com.yuxin.kvlsmtree.model.command.SetCommand;
import com.yuxin.kvlsmtree.utils.BlockUtil;
import com.yuxin.kvlsmtree.utils.ConvertUtil;
import com.yuxin.kvlsmtree.utils.FileUtil;
import com.yuxin.kvlsmtree.utils.LoggerUtil;
import lombok.Data;
import org.xerial.snappy.Snappy;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

@Data
public class SsTable implements Closeable {
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

    public static SsTable createFromIndex(Long fileNumber, int partSize,
                                          ConcurrentSkipListMap<String, Command> index,
                                          boolean enablePartDataCompress,
                                          Integer level) throws IOException {
        SsTable ssTable = new SsTable(fileNumber, partSize, enablePartDataCompress, level);
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
            LinkedList<Position> sparseKeyPositionList = new LinkedList<>();

            // find the key range including target key in sparseIndex
            for (String k : sparseIndex.keySet()) {
                if (k.compareTo(key) <= 0) {
                    sparseKeyPositionList.add(sparseIndex.get(k));
                } else {
                    break;
                }
            }
            if (sparseKeyPositionList.size() == 0) {
                return null;
            }
            LoggerUtil.debug((org.slf4j.Logger) LOGGER, "[SsTable][restoreFromFile][sparseKeyPositionList]: {}", sparseKeyPositionList);

            // 这里
            for (Position position : sparseKeyPositionList) {
                JSONObject dataPartJson = BlockUtil.readJsonObject(position, enablePartDataCompress, tableFile);
                LoggerUtil.debug((org.slf4j.Logger) LOGGER, "[SsTable][restoreFromFile][dataPartJson]: {}", dataPartJson);
                if (dataPartJson.containsKey(key)) {
                    JSONObject value = dataPartJson.getJSONObject(key);
                    return ConvertUtil.jsonToCommand(value);
                }
            }
            return null;
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
            this.sparseIndex = JSONObject.parseObject(indexStr, new TypeReference<TreeMap<String, Position>>() {});
            tableMetaInfo.setNumber(FileUtil.parseFileNumber(filePath));
            this.tableMetaInfo = tableMetaInfo;
            this.level = FileUtil.parseSsTableFileLevel(filePath);
            LoggerUtil.debug((org.slf4j.Logger) LOGGER, "[SsTable][restoreFromFile][sparseIndex]: {}", sparseIndex);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void initFromIndex(ConcurrentSkipListMap<String, Command> index) {
        try {
            JSONObject partData = new JSONObject(true);
            tableMetaInfo.setDataStart(tableFile.getFilePointer());
            for (Command command : index.values()) {
                if (command instanceof SetCommand) {
                    SetCommand set = (SetCommand) command;
                    partData.put(set.getKey(), set);
                }

                if (command instanceof RmCommand) {
                    RmCommand rm = (RmCommand) command;
                    partData.put(rm.getKey(), rm);
                }

                if (partData.size() >= tableMetaInfo.getPartSize()) {
                    writeDataPart(partData);
                }
            }

            if (partData.size() > 0) {
                writeDataPart(partData);
            }
            long dataPartLen = tableFile.getFilePointer() - tableMetaInfo.getDataStart();
            tableMetaInfo.setDataLen(dataPartLen);

            byte[] indexBytes = JSONObject.toJSONString(sparseIndex).getBytes(StandardCharsets.UTF_8);
            tableMetaInfo.setIndexStart(tableFile.getFilePointer());
            tableFile.write(indexBytes);
            tableMetaInfo.setIndexLen(indexBytes.length);
            tableMetaInfo.setSmallestKey(index.firstKey());
            tableMetaInfo.setLargestKey(index.lastKey());
            tableMetaInfo.setFileSize(dataPartLen); // 这里
            LoggerUtil.debug((org.slf4j.Logger) LOGGER, "[SsTable][initFromIndex][sparseIndex]: {}", sparseIndex);

            tableMetaInfo.writeToFile(tableFile);
            LoggerUtil.info((org.slf4j.Logger) LOGGER, "[SsTable][initFromIndex]: {},{}", filePath, tableMetaInfo);
        } catch (Throwable t) {
            LoggerUtil.debug((org.slf4j.Logger) LOGGER, "initFromIndex exception,", t);
            throw new RuntimeException(t);
        }
    }

    private void writeDataPart(JSONObject partData) throws IOException {
        byte[] partDataBytes = partData.toJSONString().getBytes(StandardCharsets.UTF_8);

        //compress(use snappy: https://github.com/xerial/snappy-java)
        if (enablePartDataCompress) {
            //LoggerUtil.debug(LOGGER, "writeDataPart,partDataBytes size:[" + partDataBytes.length + "]");
            partDataBytes = Snappy.compress(partDataBytes);
            //LoggerUtil.debug(LOGGER, "writeDataPart,partDataBytes compressed size:[" + partDataBytes.length + "]");
        }

        long start = tableFile.getFilePointer();
        //LoggerUtil.debug(LOGGER, "writeDataPart,before write FilePointer=" + start);
        tableFile.write(partDataBytes);
        //LoggerUtil.debug(LOGGER, "writeDataPart,after write FilePointer=" + tableFile.getFilePointer());

        // put the first key of data block into sparseIndex
        Optional<String> firstKey = partData.keySet().stream().findFirst();
        byte[] finalPartDataBytes = partDataBytes;
        firstKey.ifPresent(s -> sparseIndex.put(s, new Position(start, finalPartDataBytes.length)));
        partData.clear();
    }

    @Override
    public void close() throws IOException {
        tableFile.close();
    }

    @Override
    public String toString() {
        Map<String, Object> result = new HashMap<>();
        result.put("tableMetaInfo", tableMetaInfo.toString());
        result.put("level", level);
        result.put("sparseIndex", sparseIndex.toString());
        result.put("filePath", filePath);
        return result.toString();
    }

    public Long getFileNumber() {
        return this.tableMetaInfo.getNumber();
    }
}
