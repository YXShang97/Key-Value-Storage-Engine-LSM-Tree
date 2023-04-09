package com.yuxin.kvlsmtree.compaction;

import com.alibaba.fastjson.JSONObject;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yuxin.kvlsmtree.model.Position;
import com.yuxin.kvlsmtree.model.command.Command;
import com.yuxin.kvlsmtree.model.command.RmCommand;
import com.yuxin.kvlsmtree.model.sstable.SsTable;
import com.yuxin.kvlsmtree.utils.BlockUtil;
import com.yuxin.kvlsmtree.utils.ConvertUtil;
import com.yuxin.kvlsmtree.utils.LoggerUtil;
import com.yuxin.kvlsmtree.utils.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

// Compact the data in the level to the next level
public class Compactioner {

    private final Logger LOGGER = LoggerFactory.getLogger(Compactioner.class);

    // The maximum number of sstable files in the level 0
    private final static Integer L0_DUMP_MAX_FILE_NUM = 3;

    // The maximum size of sstable files in other level (unit: byte)
    private final static Integer L_OTHER_DUMP_MAX_FILE_SIZE = 100;

    // thread pool for compaction
    private ExecutorService compactionExecutor;

    // compaction result
    private Future<?> backgroundCompactionFuture;

    // Lock
    private final ReentrantLock mutex = new ReentrantLock();

    public Compactioner() {
        ThreadFactory compationThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("yuxin-kvstore-compaction-thread-%s")
                .setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler()
                {
                    @Override
                    public void uncaughtException(Thread t, Throwable e)
                    {
                        LoggerUtil.error(LOGGER, e, "Uncaught exception in compaction thread: " + t.getName());
                    }
                })
                .build();
        compactionExecutor = Executors.newSingleThreadExecutor(compationThreadFactory);
    }

    public void compaction(Map<Integer, List<SsTable>> levelMetaInfos, AtomicLong nextFileNumber) {
        mutex.lock();

        // synchronous execution
        try {
            doBackgroundCompaction(levelMetaInfos, nextFileNumber);
        } catch (Exception e) {
            LoggerUtil.error(LOGGER, e, "doBackgroundCompaction exception,", e);
        }

        // 这里
        /**
         // asynchronous execution: submit to thread pool
         this.backgroundCompactionFuture = compactionExecutor.submit(new Callable<Void>() {
        @Override
        public Void call() {
            try {
                doBackgroundCompaction(levelMetaInfos, nextFileNumber);
            } catch (Exception e) {
                System.out.println("doBackgroundCompaction,exception," + e);
                e.printStackTrace();
            }
            return null;
        }});
         **/

        mutex.unlock();
    }

    public void doBackgroundCompaction(Map<Integer, List<SsTable>> levelMetaInfos,
                                       AtomicLong nextFileNumber) throws IOException {
        // trigger compaction on l0 or not?
        if (levelMetaInfos.get(0).size() <= L0_DUMP_MAX_FILE_NUM) {
            return;
        }

        LoggerUtil.debug(LOGGER, "doBackgroundCompaction begin...levelMetaInfos=" + levelMetaInfos);

        // 1. execute compaction of sstables on level 0
        // find the sstables on level 0 that overlap with the newly generated sstable in terms of keys
        // merge it with the sstables on level 1 that overlap with this key range
        // store the merged result to the next level
        List<SsTable> l0Sstables  = levelMetaInfos.get(0);
        Optional<SsTable> maxFileNumOptional = l0Sstables.stream().max(Comparator.comparingLong(SsTable::getFileNumber));
        SsTable lastL0SsTable = maxFileNumOptional.get();

        LoggerUtil.debug(LOGGER, "doBackgroundCompaction, lastL0SsTable=" + lastL0SsTable);

        List<SsTable> overlappedL0SSTableFileMetaInfos = findOverlapSsTables(
                lastL0SsTable.getTableMetaInfo().getSmallestKey(),
                lastL0SsTable.getTableMetaInfo().getLargestKey(), l0Sstables);

        LoggerUtil.debug(LOGGER, "doBackgroundCompaction,overlappedL0SSTableFileMetaInfos=" + overlappedL0SSTableFileMetaInfos);

        // calculate the key range of the sstables overlapped
        String smallestKey = null;
        String largestKey = null;
        for (SsTable ssTableFileMetaInfo : overlappedL0SSTableFileMetaInfos) {
            if (smallestKey == null || smallestKey.compareTo(ssTableFileMetaInfo.getTableMetaInfo().getSmallestKey()) > 0) {
                smallestKey = ssTableFileMetaInfo.getTableMetaInfo().getSmallestKey();
            }
            if (largestKey == null || largestKey.compareTo(ssTableFileMetaInfo.getTableMetaInfo().getLargestKey()) < 0) {
                largestKey = ssTableFileMetaInfo.getTableMetaInfo().getLargestKey();
            }
        }

        LoggerUtil.debug(LOGGER, "doBackgroundCompaction,smallestKey=" + smallestKey + ",largestKey=" + largestKey);

        // find the sstables on level 1 overlapped with the key range
        List<SsTable> overlappedL1SSTableFileMetaInfos = findOverlapSsTables(
                smallestKey, largestKey, levelMetaInfos.get(1));

        LoggerUtil.debug(LOGGER, "doBackgroundCompaction,overlappedL1SSTableFileMetaInfos=" + overlappedL1SSTableFileMetaInfos);

        // merge the sstables on level 0 and level 1 to a new sstable
        List<SsTable> overlappedSstables = new ArrayList<>();
        if (!overlappedL0SSTableFileMetaInfos.isEmpty()) {
            overlappedSstables.addAll(overlappedL0SSTableFileMetaInfos);
        }
        if (!overlappedL1SSTableFileMetaInfos.isEmpty()) {
            overlappedSstables.addAll(overlappedL1SSTableFileMetaInfos);
        }

        overlappedSstables.sort((o1, o2) -> {
            if (o1.getFileNumber() > o2.getFileNumber()) {
                return 1;
            } else if (o1.getFileNumber() < o2.getFileNumber()) {
                return -1;
            } else {
                return 0;
            }
        });

        // result after merge
        ConcurrentSkipListMap<String, Command> mergedData = new ConcurrentSkipListMap<>();
        mergeSsTables(overlappedSstables, mergedData);

        // generate new sstable
        LoggerUtil.debug(LOGGER, "doBackgroundCompaction...generate new sstable,overlappedL0SSTableFileMetaInfos"
                + overlappedL0SSTableFileMetaInfos + ",overlappedL1SSTableFileMetaInfos="
                + overlappedL1SSTableFileMetaInfos + ",mergedData=" + mergedData);

        SsTable newSsTable = SsTable.createFromIndex(nextFileNumber.getAndIncrement(),
                4, mergedData, true, 1);
        List<SsTable> l1Sstables = levelMetaInfos.get(1);
        if (l1Sstables == null) {
            l1Sstables = new ArrayList<>();
            levelMetaInfos.put(1, l1Sstables);
        }
        l1Sstables.add(newSsTable);
        // delete sstables not needed
        // 这里
        deleteSsTables(overlappedL0SSTableFileMetaInfos, l0Sstables, 0);
        deleteSsTables(overlappedL1SSTableFileMetaInfos, l1Sstables,1);

        LoggerUtil.debug(LOGGER, "doBackgroundCompaction...finish l0 compaction,levelMetaInfos=" + levelMetaInfos);
    }

    private void deleteSsTables(List<SsTable> overlappedSSTableFileMetaInfos, List<SsTable> ssTables, int level) throws IOException {
        Iterator ssTableIterator = ssTables.iterator();
        while (ssTableIterator.hasNext()) {
            SsTable tempSsTable = (SsTable) ssTableIterator.next();
            if (containTheSsTable(overlappedSSTableFileMetaInfos, tempSsTable.getFileNumber())) {
                ssTableIterator.remove();
                tempSsTable.close();
                File tmpSstableFile = new File(FileUtil.buildSsTableFilePath(tempSsTable.getFileNumber(), level));
                tmpSstableFile.delete();
            }
        }
    }

    private boolean containTheSsTable(List<SsTable> ssTables, long fileNumber) {
        for (SsTable ssTable : ssTables) {
            if (ssTable.getFileNumber() == fileNumber) {
                return true;
            }
        }
        return false;
    }

    private void mergeSsTables(List<SsTable> ssTables, ConcurrentSkipListMap<String, Command> mergedData) {
        ssTables.forEach(ssTable -> {
            Map<String, JSONObject> jsonObjectMap = readSsTableContent(ssTable);
            for (Map.Entry<String, JSONObject> entry : jsonObjectMap.entrySet()) {
                Command command = ConvertUtil.jsonToCommand(entry.getValue());
                if (command instanceof RmCommand) {
                    mergedData.remove(command.getKey());
                } else {
                    mergedData.put(command.getKey(), command);
                }
            }
        });
    }

    private Map<String, JSONObject> readSsTableContent(SsTable ssTable) {
        Map<String, JSONObject> jsonObjectMap = new HashMap<>();

        try {
            TreeMap<String, Position> sparseIndex = ssTable.getSparseIndex();
            for (Position position : sparseIndex.values()) {
                JSONObject jsonObject = BlockUtil.readJsonObject(position, true, ssTable.getTableFile());
                for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                    String key = entry.getKey();
                    jsonObjectMap.put(key, jsonObject.getJSONObject(key));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jsonObjectMap;
    }

    private List<SsTable> findOverlapSsTables(String smallestKey, String largestKey,
                                              List<SsTable> ssTables) {
        List<SsTable> overlappedSSTableFileMetaInfos = new ArrayList<>();
        if (ssTables == null) {
            return overlappedSSTableFileMetaInfos;
        }

        for (SsTable ssTable : ssTables) {
            if (!(ssTable.getTableMetaInfo().getLargestKey().compareTo(smallestKey) < 0
                    || ssTable.getTableMetaInfo().getSmallestKey().compareTo(largestKey) > 0)) {
                overlappedSSTableFileMetaInfos.add(ssTable);
            }
        }
        return overlappedSSTableFileMetaInfos;
    }
}
