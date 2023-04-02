package com.yuxin.kvlsmtree.utils;

import com.yuxin.kvlsmtree.constants.KVConstants;
import org.apache.commons.lang3.StringUtils;

public class FileUtil {
    public final static String buildSsTableFilePath(Long fileNumber, Integer level) {
        return KVConstants.WORK_DIR + level + "_" + fileNumber + KVConstants.FILE_SUFFIX_SSTABLE;
    }

    public final static Long parseFileNumber(String filePath) {
        if (StringUtils.isBlank(filePath)) {
            return -99L;
        }

        Integer lastIndex = filePath.lastIndexOf('/');
        if (lastIndex == -1) {
            return -99L;
        }
        String fileName = filePath.substring(lastIndex + 1);
        return Long.valueOf(fileName.substring(fileName.indexOf("_") + 1, fileName.indexOf('.')));
    }

    public final static Integer parseSsTableFileLevel(String filePath) {
        if (StringUtils.isBlank(filePath)) {
            return -99;
        }

        Integer lastIndex = filePath.lastIndexOf('/');
        if (lastIndex == -1) {
            return -99;
        }

        String fileName = filePath.substring(lastIndex + 1);
        return Integer.valueOf(fileName.substring(0, fileName.indexOf('_')));
    }
}
