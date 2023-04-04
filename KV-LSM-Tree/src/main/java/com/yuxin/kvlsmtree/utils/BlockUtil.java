package com.yuxin.kvlsmtree.utils;

import com.alibaba.fastjson.JSONObject;
import com.yuxin.kvlsmtree.model.Position;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.io.RandomAccessFile;

public class BlockUtil {
    public final static JSONObject readJsonObject(Position position,
                                                  boolean enablePartDataCompress,
                                                  RandomAccessFile tableFile) throws IOException {
        // read the data block from tableFile
        byte[] dataPart = new byte[(int) position.getLen()];
        tableFile.seek(position.getStart());
        tableFile.read(dataPart);

        // uncompress the data block
        if (enablePartDataCompress) {
            dataPart = Snappy.uncompress(dataPart);
        }

        JSONObject dataPartJson = JSONObject.parseObject(new String(dataPart));
        return dataPartJson;
    }
}
