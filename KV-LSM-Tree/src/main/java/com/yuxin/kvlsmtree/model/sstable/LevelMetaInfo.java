package com.yuxin.kvlsmtree.model.sstable;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class LevelMetaInfo {

    private final Integer levelNo;
    private List<SSTableFileMetaInfo> ssTableFileMetaInfos = new ArrayList<>();

    public LevelMetaInfo(Integer levelNo){
        this.levelNo = levelNo;
    }
}

