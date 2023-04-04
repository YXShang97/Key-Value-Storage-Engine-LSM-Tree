package com.yuxin.kvlsmtree.model.sstable;

import lombok.Data;

import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;


@Data
public class TableMetaInfo {

    private long version;

    private long number;

    private long dataStart;

    private long dataLen;

    private long indexStart;

    private long indexLen;

    private long partSize;

    private long fileSize;
    private String smallestKey;
    private String largestKey;

    public void writeToFile(RandomAccessFile file) {
        try {
            file.writeBytes(smallestKey);
            file.writeInt(smallestKey.getBytes(StandardCharsets.UTF_8).length);
            file.writeBytes(largestKey);
            file.writeInt(largestKey.getBytes(StandardCharsets.UTF_8).length);

            file.writeLong(partSize);
            file.writeLong(dataStart);
            file.writeLong(dataLen);
            file.writeLong(indexStart);
            file.writeLong(indexLen);
            file.writeLong(version);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static TableMetaInfo readFromFile(RandomAccessFile file) {
        try {
            TableMetaInfo tableMetaInfo = new TableMetaInfo();
            long fileLen = file.length();

            file.seek(fileLen - 8);
            tableMetaInfo.setVersion(file.readLong());

            file.seek(fileLen - 8 * 2);
            tableMetaInfo.setIndexLen(file.readLong());

            file.seek(fileLen - 8 * 3);
            tableMetaInfo.setIndexStart(file.readLong());

            file.seek(fileLen - 8 * 4);
            tableMetaInfo.setDataLen(file.readLong());

            file.seek(fileLen - 8 * 5);
            tableMetaInfo.setDataStart(file.readLong());

            file.seek(fileLen - 8 * 6);
            tableMetaInfo.setPartSize(file.readLong());

            file.seek(fileLen - 8 * 6 - 4);
            Integer largestKeyLength = file.readInt();
            file.seek(fileLen - 8 * 6 - 4 - largestKeyLength);
            byte[] largestKeyBytes = new byte[largestKeyLength];
            file.read(largestKeyBytes);
            tableMetaInfo.setLargestKey(new String(largestKeyBytes, StandardCharsets.UTF_8));

            file.seek(fileLen - 8 * 6 - 8 - largestKeyLength);
            Integer smallestKeyLength = file.readInt();
            file.seek(fileLen - 8 * 6 - 8 - largestKeyLength - smallestKeyLength);
            byte[] smallestKeyBytes = new byte[smallestKeyLength];
            file.read(smallestKeyBytes);
            tableMetaInfo.setSmallestKey(new String(smallestKeyBytes));

            return tableMetaInfo;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
