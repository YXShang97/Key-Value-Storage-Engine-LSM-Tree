package com.yuxin.kvlsmtree.service;

import com.alibaba.fastjson.JSONObject;
import com.yuxin.kvlsmtree.constants.KVConstants;
import com.yuxin.kvlsmtree.model.command.Command;
import com.yuxin.kvlsmtree.model.command.SetCommand;
import com.yuxin.kvlsmtree.model.sstable.SsTable;
import com.yuxin.kvlsmtree.utils.ConvertUtil;
import com.yuxin.kvlsmtree.utils.LoggerUtil;
import lombok.Getter;

import javax.smartcardio.CommandAPDU;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

public class LsmKVStore implements KVStore{

    private final Logger LOGGER = Logger.getLogger(LsmKVStore.class.getName());
    private static final String WAL = "wal";
    private static final String RW_MODE = "rw";
    private static final String WAL_TMP = "walTmp";

    private ConcurrentSkipListMap<String, Command> memTable;

    private ConcurrentSkipListMap<String, Command> immutableMemTable;

    @Getter
    private Map<Integer, List<SsTable>> levelMetaInfos;

    private final String dataDir;

    private final ReadWriteLock indexLock;

    private final int storeThreshold;

    private final int partSize;

    private RandomAccessFile wal;

    private File walFile;

    private final AtomicLong nextFileNumber = new AtomicLong(1);

    private Compactioner compactioner;

    public LsmKVStore(String dataDir, int storeThreshold, int partSize) {
        try {
            this.dataDir = dataDir;
            File dir = new File(dataDir);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            File[] files = dir.listFiles();

            this.storeThreshold = storeThreshold;
            this.partSize = partSize;
            this.indexLock = new ReentrantReadWriteLock();
            this.memTable = new ConcurrentSkipListMap<>();
            this.levelMetaInfos = new ConcurrentHashMap<>();
            this.memTable = new ConcurrentSkipListMap<>();
            this.walFile = new File(dataDir + WAL);
            this.wal = new RandomAccessFile(walFile, RW_MODE);
            this.compactioner = new Compactioner();

            // if directory is empty, need to load ssTable
            if (files == null || files.length == 0) {
                return;
            }

            // load data from ssTable and wal
            for (File file : files) {
                String fileName = file.getName();
                // restore data from wal
                if (file.isFile() && fileName.equals(WAL_TMP)) {  // recover data from walTmp
                    restoreFromWal(new RandomAccessFile(file, RW_MODE));
                } else if (file.isFile() && fileName.equals(WAL)) {
                    walFile = file;
                    wal = new RandomAccessFile(walFile, RW_MODE);
                    restoreFromWal(wal);
                }
                // load data from ssTable
                if (file.isFile() && fileName.endsWith(KVConstants.FILE_SUFFIX_SSTABLE)) {
                    SsTable ssTable = SsTable.createFromFile(file.getAbsolutePath(), true);
                    Integer level = ssTable.getLevel();

                    // 这里
                }

            }
            // 这里
        } catch (Throwable t) {
            LoggerUtil.error((org.slf4j.Logger) LOGGER,t, "init error~");
            throw new RuntimeException(t);
        }
    }

    private void restoreFromWal(RandomAccessFile wal) {
        try {
            long len = wal.length();
            long start = 0;
            wal.seek(start);
            while (start < len) {
                int commandLen = wal.readInt();
                byte[] commandBytes = new byte[commandLen];
                wal.read(commandBytes);
                JSONObject commandJson = JSONObject.parseObject(new String(commandBytes));
                Command command = ConvertUtil.jsonToCommand(commandJson);
                if (command == null) {
                    memTable.put(command.getKey(), command);
                }
                start += commandLen + 4;
            }
            wal.seek(wal.length());
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public void set(String key, String value) {
        try {
            SetCommand command = new SetCommand(key, value);
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            indexLock.writeLock().lock();
            wal.writeInt(commandBytes.length);
            wal.write(commandBytes);
            memTable.put(key, command);

            if (memTable.size() > storeThreshold) {
                switchIndex();
                dumpToL0SsTable();
            }
        } catch (Throwable t) {
            LoggerUtil.error((org.slf4j.Logger) LOGGER, t, "command set error~");
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    private void switchIndex() {
        try {
            indexLock.writeLock().lock();
            if (immutableMemTable != null) {
                return;
            }
            immutableMemTable = memTable;
            memTable = new ConcurrentSkipListMap<>();
            wal.close();

            File walTmp = new File(dataDir + WAL_TMP);
            if (walTmp.exists()) {
                if (!walTmp.delete()) {
                    throw new RuntimeException("delete walTmp error~");
                }
            }
            if (!walFile.renameTo(walTmp)) {
                throw new RuntimeException("rename wal error~");
            }

            walFile = new File(dataDir + WAL);
            wal = new RandomAccessFile(walFile, RW_MODE);
        } catch (Throwable t) {
            LoggerUtil.error((org.slf4j.Logger) LOGGER, t, "switch index error~");
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }

    private void dumpToL0SsTable() {
        try {
            Long fileNumber = nextFileNumber.getAndIncrement();
            // 这里
        } catch (Throwable t) {
            LoggerUtil.error((org.slf4j.Logger) LOGGER,t, "dump to l0 ssTable error~");
            throw new RuntimeException(t);
        }
    }

    @Override
    public String get(String key) {
        return null;
    }

    @Override
    public void rm(String key) {

    }

    @Override
    public void printfStats() {

    }

    @Override
    public void close() throws IOException {

    }
}
