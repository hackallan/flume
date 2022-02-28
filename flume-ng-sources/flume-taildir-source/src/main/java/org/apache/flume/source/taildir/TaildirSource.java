/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flume.source.taildir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import org.apache.flume.*;
import org.apache.flume.client.avro.ReliableSpoolingFileEventReader;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.serialization.DecodeErrorPolicy;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.PollableSourceConstants;
import org.apache.flume.source.SpoolDirectorySource;
import org.apache.flume.source.SpoolDirectorySourceConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;

import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.*;
import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.DEFAULT_CLEAR_LOG_INTERVAL;

public class TaildirSource extends AbstractSource implements
        PollableSource, Configurable, BatchSizeSupported {

  private static final Logger logger = LoggerFactory.getLogger(TaildirSource.class);

  private Map<String, String> filePaths;
  private Table<String, String, String> headerTable;
  private int batchSize;
  private String positionFilePath;
  private String rmFilePath;
  private boolean skipToEnd;
  private boolean byteOffsetHeader;

  private SourceCounter sourceCounter;
  private ReliableTaildirEventReader reader;
  private ScheduledExecutorService idleFileChecker;
  private ScheduledExecutorService positionWriter;
  private ScheduledExecutorService logClearService;
  private ScheduledExecutorService spoolDirService;

  private int retryInterval = 1000;
  private int maxRetryInterval = 5000;
  private int idleTimeout;
  private int checkIdleInterval = 5000;
  private int writePosInitDelay = 5000;
  private int logProcessInitDelay = 0;
  private int spoolDirInitDelay = 0;
  private int writePosInterval;
  private int logInterval;
  private int spoolDirInterval;
  private int clearLogInterval;
  private boolean cachePatternMatching;

  private List<Long> existingInodes = new CopyOnWriteArrayList<Long>();
  private List<Long> idleInodes = new CopyOnWriteArrayList<Long>();
  private Long backoffSleepIncrement;
  private Long maxBackOffSleepInterval;
  private boolean fileHeader;
  private String fileHeaderKey;
  private Long maxBatchCount;
  /**
   * spool dir 参数
   */
  private Context spoolContext;
  private SpoolDirectorySource spoolSource;
  private ReliableSpoolingFileEventReader spoolReader;
  private String completedSuffix;
  private String deletePolicy;
  private String spoolDirectory;
  private Boolean basenameHeader;
  private String basenameHeaderKey;
  private String inputCharset;
  private DecodeErrorPolicy decodeErrorPolicy;
  private String includePattern;
  private String ignorePattern;
  private String trackerDirPath;
  private String deserializerType;
  private Context deserializerContext;
  private SpoolDirectorySourceConfigurationConstants.ConsumeOrder consumeOrder;
  private Integer pollDelay;
  private Boolean recursiveDirectorySearch;
  private Integer maxBackoff;
  private String trackingPolicy;
  private boolean backoff = true;
  private boolean hitChannelFullException = false;
  private boolean hitChannelException = false;
  private volatile boolean hasFatalError = false;

  @Override
  public synchronized void start() {
    logger.info("{} TaildirSource source starting with directory: {}", getName(), filePaths);
    try {
      reader = new ReliableTaildirEventReader.Builder()
              .filePaths(filePaths)
              .headerTable(headerTable)
              .positionFilePath(positionFilePath)
              .skipToEnd(skipToEnd)
              .addByteOffset(byteOffsetHeader)
              .cachePatternMatching(cachePatternMatching)
              .annotateFileName(fileHeader)
              .fileNameHeader(fileHeaderKey)
              .newFilePath(rmFilePath)
              .build();
    } catch (IOException e) {
      throw new FlumeException("Error instantiating ReliableTaildirEventReader", e);
    }
    idleFileChecker = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("idleFileChecker").build());
    idleFileChecker.scheduleWithFixedDelay(new idleFileCheckerRunnable(),
            idleTimeout, checkIdleInterval, TimeUnit.MILLISECONDS);

    positionWriter = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("positionWriter").build());
    positionWriter.scheduleWithFixedDelay(new PositionWriterRunnable(),
            writePosInitDelay, writePosInterval, TimeUnit.MILLISECONDS);

    //开启新线程用于处理access log
    if (rmFilePath != null) {
      logClearService = Executors.newSingleThreadScheduledExecutor(
              new ThreadFactoryBuilder().setNameFormat("logClearService").build());
      logClearService.scheduleWithFixedDelay(new LogProcessRunnable(),
              logProcessInitDelay, logInterval, TimeUnit.HOURS);
    }

    spoolDirService = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("spoolDirService").build());

    spoolDirService.scheduleWithFixedDelay(new LogAgainProcessRunnable(),
            spoolDirInitDelay, spoolDirInterval, TimeUnit.SECONDS);

    super.start();
    logger.debug("TaildirSource started");
    sourceCounter.start();
  }

  @Override
  public synchronized void stop() {
    try {
      super.stop();
      ExecutorService[] services = {idleFileChecker, positionWriter, logClearService, spoolDirService};
      for (ExecutorService service : services) {
        if (service != null) {
          service.shutdown();

          if (!service.awaitTermination(1, TimeUnit.SECONDS)) {
            service.shutdownNow();
          }
        }


      }
      // write the last position
      writePosition();
      reader.close();
    } catch (InterruptedException e) {
      logger.info("Interrupted while awaiting termination", e);
    } catch (IOException e) {
      logger.info("Failed: " + e.getMessage(), e);
    }
    sourceCounter.stop();
    logger.info("Taildir source {} stopped. Metrics: {}", getName(), sourceCounter);
  }

  @Override
  public String toString() {
    return String.format("Taildir source: { positionFile: %s, skipToEnd: %s, "
                    + "byteOffsetHeader: %s, idleTimeout: %s, writePosInterval: %s }",
            positionFilePath, skipToEnd, byteOffsetHeader, idleTimeout, writePosInterval);
  }

  @Override
  public synchronized void configure(Context context) {
    this.spoolContext = context;//为了初始spoodir source准备上下文
    String fileGroups = context.getString(FILE_GROUPS);
    Preconditions.checkState(fileGroups != null, "Missing param: " + FILE_GROUPS);

    filePaths = selectByKeys(context.getSubProperties(FILE_GROUPS_PREFIX),
            fileGroups.split("\\s+"));
    Preconditions.checkState(!filePaths.isEmpty(),
            "Mapping for tailing files is empty or invalid: '" + FILE_GROUPS_PREFIX + "'");

    String homePath = System.getProperty("user.home").replace('\\', '/');
    positionFilePath = context.getString(POSITION_FILE, homePath + DEFAULT_POSITION_FILE);
    Path positionFile = Paths.get(positionFilePath);


    try {

      rmFilePath = context.getString(RM_FILE_PATH_DIR);
      if (rmFilePath != null) {
        Path rmFile = Paths.get(rmFilePath);
        Files.createDirectories(rmFile);
      }

      Files.createDirectories(positionFile.getParent());
    } catch (IOException e) {
      throw new FlumeException("Error creating positionFile parent directories", e);
    }
    headerTable = getTable(context, HEADERS_PREFIX);
    batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    skipToEnd = context.getBoolean(SKIP_TO_END, DEFAULT_SKIP_TO_END);
    byteOffsetHeader = context.getBoolean(BYTE_OFFSET_HEADER, DEFAULT_BYTE_OFFSET_HEADER);
    idleTimeout = context.getInteger(IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT);
    writePosInterval = context.getInteger(WRITE_POS_INTERVAL, DEFAULT_WRITE_POS_INTERVAL);
    logInterval = context.getInteger(LOG_INTERVAL, DEFAULT_RM_INTERVAL);
    spoolDirInterval = context.getInteger(SPOOL_DIR_INTERVAL, DEFAULT_SPOOL_DIR_INTERVAL);
    clearLogInterval = context.getInteger(CLEAR_LOG_INTERVAL, DEFAULT_CLEAR_LOG_INTERVAL);
    cachePatternMatching = context.getBoolean(CACHE_PATTERN_MATCHING,
            DEFAULT_CACHE_PATTERN_MATCHING);

    backoffSleepIncrement = context.getLong(PollableSourceConstants.BACKOFF_SLEEP_INCREMENT,
            PollableSourceConstants.DEFAULT_BACKOFF_SLEEP_INCREMENT);
    maxBackOffSleepInterval = context.getLong(PollableSourceConstants.MAX_BACKOFF_SLEEP,
            PollableSourceConstants.DEFAULT_MAX_BACKOFF_SLEEP);
    fileHeader = context.getBoolean(FILENAME_HEADER,
            DEFAULT_FILE_HEADER);
    fileHeaderKey = context.getString(FILENAME_HEADER_KEY,
            DEFAULT_FILENAME_HEADER_KEY);
    maxBatchCount = context.getLong(MAX_BATCH_COUNT, DEFAULT_MAX_BATCH_COUNT);
    if (maxBatchCount <= 0) {
      maxBatchCount = DEFAULT_MAX_BATCH_COUNT;
      logger.warn("Invalid maxBatchCount specified, initializing source "
              + "default maxBatchCount of {}", maxBatchCount);
    }

    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }


    //spool dir 配置

    this.spoolDirectory = context.getString("spoolDir");
    Preconditions.checkState(this.spoolDirectory != null, "Configuration must specify a spooling directory");
    this.completedSuffix = context.getString("fileSuffix", ".COMPLETED");
    this.deletePolicy = context.getString("deletePolicy", "never");
    this.fileHeader = context.getBoolean("fileHeader", false);
    this.fileHeaderKey = context.getString("fileHeaderKey", "file");
    this.basenameHeader = context.getBoolean("basenameHeader", false);
    this.basenameHeaderKey = context.getString("basenameHeaderKey", "basename");
    this.batchSize = context.getInteger("batchSize", 100);
    this.inputCharset = context.getString("inputCharset", "UTF-8");
    this.decodeErrorPolicy = DecodeErrorPolicy.valueOf(context.getString("decodeErrorPolicy", SpoolDirectorySourceConfigurationConstants.DEFAULT_DECODE_ERROR_POLICY).toUpperCase(Locale.ENGLISH));
    this.includePattern = context.getString("includePattern", "^.*$");
    this.ignorePattern = context.getString("ignorePattern", "^$");
    this.trackerDirPath = context.getString("trackerDir", ".flumespool");
    this.deserializerType = context.getString("deserializer", "LINE");
    this.deserializerContext = new Context(context.getSubProperties("deserializer."));
    this.consumeOrder = SpoolDirectorySourceConfigurationConstants.ConsumeOrder.valueOf(context.getString("consumeOrder", SpoolDirectorySourceConfigurationConstants.DEFAULT_CONSUME_ORDER.toString()).toUpperCase(Locale.ENGLISH));
    this.pollDelay = context.getInteger("pollDelay", 500);
    this.recursiveDirectorySearch = context.getBoolean("recursiveDirectorySearch", false);
    Integer bufferMaxLineLength = context.getInteger("bufferMaxLineLength");
    if (bufferMaxLineLength != null && this.deserializerType != null && this.deserializerType.equalsIgnoreCase("LINE")) {
      this.deserializerContext.put("maxLineLength", bufferMaxLineLength.toString());
    }

    this.maxBackoff = context.getInteger("maxBackoff", SpoolDirectorySourceConfigurationConstants.DEFAULT_MAX_BACKOFF);
    if (this.sourceCounter == null) {
      this.sourceCounter = new SourceCounter(this.getName());
    }

    this.trackingPolicy = context.getString("trackingPolicy", "rename");
  }

  @Override
  public long getBatchSize() {
    return batchSize;
  }

  private Map<String, String> selectByKeys(Map<String, String> map, String[] keys) {
    Map<String, String> result = Maps.newHashMap();
    for (String key : keys) {
      if (map.containsKey(key)) {
        result.put(key, map.get(key));
      }
    }
    return result;
  }

  private Table<String, String, String> getTable(Context context, String prefix) {
    Table<String, String, String> table = HashBasedTable.create();
    for (Entry<String, String> e : context.getSubProperties(prefix).entrySet()) {
      String[] parts = e.getKey().split("\\.", 2);
      table.put(parts[0], parts[1], e.getValue());
    }
    return table;
  }

  @VisibleForTesting
  protected SourceCounter getSourceCounter() {
    return sourceCounter;
  }

  @Override
  public Status process() {
    Status status = Status.BACKOFF;
    try {
      existingInodes.clear();
      existingInodes.addAll(reader.updateTailFiles());
      for (long inode : existingInodes) {
        TailFile tf = reader.getTailFiles().get(inode);
        if (tf.needTail()) {
          boolean hasMoreLines = tailFileProcess(tf, true);
          if (hasMoreLines) {
            status = Status.READY;
          }
        }
      }
      closeTailFiles();
    } catch (Throwable t) {
      logger.error("Unable to tail files", t);
      sourceCounter.incrementEventReadFail();
      status = Status.BACKOFF;
    }
    return status;
  }

  @Override
  public long getBackOffSleepIncrement() {
    return backoffSleepIncrement;
  }

  @Override
  public long getMaxBackOffSleepInterval() {
    return maxBackOffSleepInterval;
  }

  private boolean tailFileProcess(TailFile tf, boolean backoffWithoutNL)
          throws IOException, InterruptedException {
    long batchCount = 0;
    while (true) {
      reader.setCurrentFile(tf);
      List<Event> events = reader.readEvents(batchSize, backoffWithoutNL);
      if (events.isEmpty()) {
        return false;
      }
      sourceCounter.addToEventReceivedCount(events.size());
      sourceCounter.incrementAppendBatchReceivedCount();
      try {
        getChannelProcessor().processEventBatch(events);
        reader.commit();
      } catch (ChannelException ex) {
        logger.warn("The channel is full or unexpected failure. " +
                "The source will try again after " + retryInterval + " ms");
        sourceCounter.incrementChannelWriteFail();
        TimeUnit.MILLISECONDS.sleep(retryInterval);
        retryInterval = retryInterval << 1;
        retryInterval = Math.min(retryInterval, maxRetryInterval);
        continue;
      }
      retryInterval = 1000;
      sourceCounter.addToEventAcceptedCount(events.size());
      sourceCounter.incrementAppendBatchAcceptedCount();
      if (events.size() < batchSize) {
        logger.debug("The events taken from " + tf.getPath() + " is less than " + batchSize);
        return false;
      }
      if (++batchCount >= maxBatchCount) {
        logger.debug("The batches read from the same file is larger than " + maxBatchCount);
        return true;
      }
    }
  }

  private void closeTailFiles() throws IOException, InterruptedException {
    for (long inode : idleInodes) {
      TailFile tf = reader.getTailFiles().get(inode);
      if (tf.getRaf() != null) { // when file has not closed yet
        tailFileProcess(tf, false);
        tf.close();
        logger.info("Closed file: " + tf.getPath() + ", inode: " + inode + ", pos: " + tf.getPos());
      }
    }
    idleInodes.clear();
  }

  /**
   * Runnable class that checks whether there are files which should be closed.
   */
  private class idleFileCheckerRunnable implements Runnable {
    @Override
    public void run() {
      try {
        long now = System.currentTimeMillis();
        for (TailFile tf : reader.getTailFiles().values()) {
          if (tf.getLastUpdated() + idleTimeout < now && tf.getRaf() != null) {
            idleInodes.add(tf.getInode());
          }
        }
      } catch (Throwable t) {
        logger.error("Uncaught exception in IdleFileChecker thread", t);
        sourceCounter.incrementGenericProcessingFail();
      }
    }
  }

  /**
   * Runnable class that writes a position file which has the last read position
   * of each file.
   */
  private class PositionWriterRunnable implements Runnable {
    @Override
    public void run() {
      writePosition();
    }
  }

  private class LogProcessRunnable implements Runnable {

    @Override
    public void run() {
      System.out.println("开始执行日志处理");
      logProcess();
      clearLog();
    }
  }

  /**
   * 日志重发
   */
  private class LogAgainProcessRunnable implements Runnable {

    @Override
    public void run() {
      retryLogProcess();
    }
  }

  /**
   * 日志重发机制
   */
  private void retryLogProcess() {


    if (this.spoolSource == null) {

      System.out.println("执行retryLogProcess");

      this.spoolSource = new SpoolDirectorySource();

      Configurables.configure(this.spoolSource, spoolContext);


      File directory = new File(this.spoolDirectory);

      try {
        this.spoolReader = (new ReliableSpoolingFileEventReader.Builder()).spoolDirectory(directory).completedSuffix(this.completedSuffix).includePattern(this.includePattern).ignorePattern(this.ignorePattern).trackerDirPath(this.trackerDirPath).annotateFileName(this.fileHeader).fileNameHeader(this.fileHeaderKey).annotateBaseName(this.basenameHeader).baseNameHeader(this.basenameHeaderKey).deserializerType(this.deserializerType).deserializerContext(this.deserializerContext).deletePolicy(this.deletePolicy).inputCharset(this.inputCharset).decodeErrorPolicy(this.decodeErrorPolicy).consumeOrder(this.consumeOrder).recursiveDirectorySearch(this.recursiveDirectorySearch).trackingPolicy(this.trackingPolicy).sourceCounter(this.sourceCounter).build();
      } catch (IOException var3) {

        throw new FlumeException("Error instantiating spooling event parser", var3);
      }

    }

    Runnable runner = new SpoolDirectoryRunnable(this.spoolReader, this.sourceCounter);
    runner.run();
    System.out.println("SpoolDirectorySource source starting with directory: {}" + this.spoolDirectory);
  }

  private void clearLog() {
    try {
      Path path = Paths.get(rmFilePath);
      System.out.println("开始执行清理工作校验 " + rmFilePath + " ,path = " + path);
      if (!Files.exists(path)) {
        return;
      }

      DirectoryStream<Path> paths = Files.newDirectoryStream(path);
      System.out.println("开始执行清理工作校验 " + paths);
      if (paths != null) {
        Iterator<Path> iterator = paths.iterator();
        while (iterator.hasNext()) {
          Path item = iterator.next();
          String str = item.toString();
          System.out.println("开始执行清理工作，当前清理天数 " + logInterval + " 天前，目录位置 " + str);
          String date = str.substring(str.indexOf("."));
          String oldDay = LocalDate.now().minusDays(clearLogInterval).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
          if (date.contains(oldDay)) {
            //清楚指定的天数
            Files.delete(item);
          }
        }
      }

    } catch (Exception e) {
      System.out.println("日志清理异常 " + e.getMessage());
    }

  }

  /**
   * process nginx access log
   */
  private void logProcess() {

    try {

      List<Long> longs = reader.updateTailFiles(true);
      longs.remove(longs.size() - 1);//移除最后一个 有可能最后一个文件还在读取中
      System.out.println("日志移动操作：" + longs.size());
      if (!longs.isEmpty()) {

        for (Long inode : longs) {
          TailFile tf = reader.getTailFiles().get(inode);
          //posInfos.add(ImmutableMap.of("inode", inode, "pos", tf.getPos(), "file", tf.getPath()));
          //System.out.println("开始执行移动工作一共，" + longs.size() + " 文件");
          String name = tf.getPath().substring(tf.getPath().lastIndexOf("/"));
          Path target = Paths.get(rmFilePath + name);
          Path source = Paths.get(tf.getPath());
          Files.move(source, target);
        }
      }
    } catch (Throwable t) {
      logger.error("Failed writing positionFile", t);
      //System.out.println("日志处理异常了" + t.getMessage());
    }
  }

  private void writePosition() {
    File file = new File(positionFilePath);
    FileWriter writer = null;
    try {
      writer = new FileWriter(file);
      if (!existingInodes.isEmpty()) {
        String json = toPosInfoJson();
        writer.write(json);
      }
    } catch (Throwable t) {
      logger.error("Failed writing positionFile", t);
      sourceCounter.incrementGenericProcessingFail();
    } finally {
      try {
        if (writer != null) writer.close();
      } catch (IOException e) {
        logger.error("Error: " + e.getMessage(), e);
        sourceCounter.incrementGenericProcessingFail();
      }
    }
  }

  private String toPosInfoJson() {
    @SuppressWarnings("rawtypes")
    List<Map> posInfos = Lists.newArrayList();
    for (Long inode : existingInodes) {
      TailFile tf = reader.getTailFiles().get(inode);
      posInfos.add(ImmutableMap.of("inode", inode, "pos", tf.getPos(), "file", tf.getPath()));
    }
    return new Gson().toJson(posInfos);
  }

  @VisibleForTesting
  protected class SpoolDirectoryRunnable implements Runnable {
    private ReliableSpoolingFileEventReader reader;
    private SourceCounter sourceCounter;


    public SpoolDirectoryRunnable(ReliableSpoolingFileEventReader reader, SourceCounter sourceCounter) {
      this.reader = reader;
      this.sourceCounter = sourceCounter;
    }

    public void run() {
      int backoffInterval = 250;
      boolean readingEvents = false;

      try {
        while (!Thread.interrupted()) {
          readingEvents = true;
          List<Event> events = this.reader.readEvents(batchSize);
          readingEvents = false;
          if (events.isEmpty()) {
            break;
          }

          this.sourceCounter.addToEventReceivedCount((long) events.size());
          this.sourceCounter.incrementAppendBatchReceivedCount();

          try {
            getChannelProcessor().processEventBatch(events);
            this.reader.commit();
          } catch (ChannelFullException var5) {
            logger.warn("The channel is full, and cannot write data now. The source will try again after " + backoffInterval + " milliseconds");
            this.sourceCounter.incrementChannelWriteFail();

            hitChannelFullException = true;
            backoffInterval = this.waitAndGetNewBackoffInterval(backoffInterval);
            continue;
          } catch (ChannelException var6) {
            logger.warn("The channel threw an exception, and cannot write data now. The source will try again after " + backoffInterval + " milliseconds");
            this.sourceCounter.incrementChannelWriteFail();
            hitChannelException = true;
            backoffInterval = this.waitAndGetNewBackoffInterval(backoffInterval);
            continue;
          }

          backoffInterval = 250;
          this.sourceCounter.addToEventAcceptedCount((long) events.size());
          this.sourceCounter.incrementAppendBatchAcceptedCount();
        }
      } catch (Throwable var7) {
        logger.error("FATAL: " + this.toString() + ": Uncaught exception in SpoolDirectorySource thread. Restart or reconfigure Flume to continue processing.", var7);
        if (readingEvents) {
          this.sourceCounter.incrementEventReadFail();
        } else {
          this.sourceCounter.incrementGenericProcessingFail();
        }

        hasFatalError = true;
        Throwables.propagate(var7);
      }

    }

    private int waitAndGetNewBackoffInterval(int backoffInterval) throws InterruptedException {
      if (backoff) {
        TimeUnit.MILLISECONDS.sleep((long) backoffInterval);
        backoffInterval <<= 1;
        backoffInterval = backoffInterval >= maxBackoff ? maxBackoff : backoffInterval;
      }

      return backoffInterval;
    }
  }
}
