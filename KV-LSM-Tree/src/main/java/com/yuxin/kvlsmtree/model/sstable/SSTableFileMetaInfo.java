package com.yuxin.kvlsmtree.model.sstable;

import lombok.Data;
@Data
public class SSTableFileMetaInfo {
    private final long fileNum;
    private final long fileSize;
    private final String smallestKey;
    private final String largestKey;

    public SSTableFileMetaInfo(long fileNum, long fileSize, String smallestKey, String largestKey) {
        this.fileNum = fileNum;
        this.fileSize = fileSize;
        this.smallestKey = smallestKey;
        this.largestKey = largestKey;
    }
}
