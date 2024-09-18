package com.alibaba.datax.plugin.reader.hdfsreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.reader.ColumnEntry;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.google.common.collect.Lists;
import com.webank.wedatasphere.exchangis.datax.core.transport.stream.ChannelOutput;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.mapred.JobConf;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.hive.PaimonSerDe;
import org.apache.paimon.hive.RowDataContainer;
import org.apache.paimon.hive.mapred.PaimonInputSplit;
import org.apache.paimon.hive.mapred.PaimonRecordReader;
import org.apache.paimon.hive.objectinspector.PaimonInternalRowObjectInspector;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author wangxl
 */
public class HdfsWithPaimonReader extends HdfsReader {

    private Boolean isPaimonTable = false;

    /**
     * Job 中的方法仅执行一次，task 中方法会由框架启动多个 task 线程并行执行。
     * <p/>
     * 整个 Reader 执行流程是：
     * <pre>
     * Job类init-->prepare-->split
     *
     * Task类init-->prepare-->startRead-->post-->destroy
     * Task类init-->prepare-->startRead-->post-->destroy
     *
     * Job类post-->destroy
     * </pre>
     */
    public static class Job extends HdfsReader.Job {
        @Override
        public void prepare() {
            //no need to prepare file in paimon model
        }

        /**
         * just follow the framework the split
         */
        @Override
        public List<Configuration> split(int adviceNumber) {
            return Lists.newArrayList(super.getPluginJobConf());
        }
    }

    public static class Task extends HdfsReader.Task {


        private static Logger LOG = LoggerFactory.getLogger(HdfsWithPaimonReader.Task.class);
        String encoding;
        HdfsReaderUtil hdfsReaderUtil = null;
        int bufferSize;
        FileSystem fileSystem;
        String specifiedFileType;
        Configuration taskConfig;
        org.apache.hadoop.conf.Configuration hadoopConf;
        String sourceFile;
        TaskPluginCollector taskPluginCollector;

        private List<Object> sourceFiles;

        @Override
        public void init() {
            this.taskConfig = super.getPluginJobConf();
            this.sourceFiles = this.taskConfig.getList(Constant.SOURCE_FILES, Object.class);
            this.specifiedFileType = this.taskConfig.getString(Key.FILETYPE, "");
            this.encoding = this.taskConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.ENCODING, "UTF-8");
            this.hdfsReaderUtil = new HdfsReaderUtil(this.taskConfig);
            this.bufferSize = this.taskConfig.getInt(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.BUFFER_SIZE,
                    com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_BUFFER_SIZE);
            try {
                this.hadoopConf = (org.apache.hadoop.conf.Configuration) FieldUtils.readField(this.hdfsReaderUtil, "hadoopConf", true);
                this.fileSystem = (FileSystem) FieldUtils.readField(this.hdfsReaderUtil, "fileSystem", true);
                this.taskPluginCollector = getTaskPluginCollector();
                this.sourceFile = taskConfig.getString("path");

            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public void startRead(RecordSender recordSender) {
            try {
                if (!isPaimonDir()) {
                    super.startRead(recordSender);
                } else {
                    LOG.info(String.format("Start Read Paimon File [%s].", sourceFile));
                    List<ColumnEntry> column = UnstructuredStorageReaderUtil
                            .getListColumnEntry(taskConfig, com.alibaba.datax.plugin.unstructuredstorage.reader.Key.COLUMN);
                    String nullFormat = taskConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.NULL_FORMAT);
                    StringBuilder allColumns = new StringBuilder();
                    StringBuilder allColumnTypes = new StringBuilder();
                    boolean isReadAllColumns = false;
                    List<Object> columnList = taskConfig.getList("column");
                    int columnIndexMax = columnList.size() - 1;
                    for (int i = 0; i <= columnList.size() - 1; i++) {
                        LinkedHashMap<String, String> columnMap = (LinkedHashMap<String, String>) columnList.get(i);
                        allColumns.append(columnMap.get("name"));
                        allColumnTypes.append("string");
                        if (i != columnIndexMax) {
                            allColumns.append(",");
                            allColumnTypes.append(":");
                        }
                    }

                    JobConf conf = new JobConf(hadoopConf);
                    Properties p = new Properties();
                    p.setProperty("columns", allColumns.toString());
                    p.setProperty("columns.types", allColumnTypes.toString());
                    PaimonSerDe serde = new PaimonSerDe();
                    serde.initialize(conf, p);
                    PaimonInternalRowObjectInspector inspector = (PaimonInternalRowObjectInspector) serde.getObjectInspector();
                    String warehouse = new Path(taskConfig.getString("defaultFS") + taskConfig.getString("path")).getParent().getParent().toString();
                    HiveConf hiveConf =
                            HiveCatalog.createHiveConf(
                                    System.getProperty("HIVE_CONF_DIR"), null, HadoopUtils.getHadoopConfiguration(new Options()));
                    String metastoreClientClass = HiveMetaStoreClient.class.getName();
                    Options catalogOptions = new Options();
                    catalogOptions.set(CatalogOptions.WAREHOUSE, warehouse);
                    CatalogContext catalogContext = CatalogContext.create(catalogOptions);
                    FileIO fileIO = FileIO.get(new org.apache.paimon.fs.Path(warehouse), catalogContext);
                    HiveCatalog catalog = new HiveCatalog(fileIO, hiveConf, metastoreClientClass, warehouse);

                    Identifier tableIdentifier = Identifier.create(taskConfig.getString("hiveDatabase"), taskConfig.getString("hiveTable"));
                    org.apache.paimon.table.Table table = catalog.getTable(tableIdentifier);
                    List<Split> splits = table.newReadBuilder().newScan().plan().splits();
                    for (Split split : splits) {
                        DataSplit dataSplit = (DataSplit) split;
                        List<String> originalColumns = ((FileStoreTable) table).schema().fieldNames();
                        PaimonRecordReader reader = new PaimonRecordReader(
                                table.newReadBuilder(),
                                new PaimonInputSplit(taskConfig.getString("path"), dataSplit, (FileStoreTable) table),
                                originalColumns,
                                originalColumns,
                                Arrays.stream(StringUtils.split(allColumns.toString(), ",")).collect(Collectors.toList()),
                                null);

                        Void key = reader.createKey();
                        RowDataContainer value = reader.createValue();
                        // get all field refs
                        List<? extends StructField> fields = inspector.getAllStructFieldRefs();

                        List<Object> recordFields;
                        while (reader.next(key, value)) {
                            recordFields = new ArrayList<>();
                            InternalRow row = value.get();
                            for (int i = 0; i <= columnIndexMax; i++) {
                                Object field = inspector.getStructFieldData(row, fields.get(i));
                                recordFields.add(field);
                            }
                            MethodUtils.invokeMethod(hdfsReaderUtil, true, "transportOneRecord", column, recordFields, recordSender, taskPluginCollector, isReadAllColumns, nullFormat);
                        }
                        reader.close();
                    }
                }

            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                String message = String.format("文件路径[%s]中读取数据发生异常，请联系系统管理员。", sourceFile);
                throw DataXException.asDataXException(HdfsReaderErrorCode.READ_FILE_ERROR, message);
            }

        }

        /**
         * just check the folder struct
         *
         * @return
         * @throws IOException
         */
        private boolean isPaimonDir() throws IOException {
            List<String> dirNames = Arrays.stream(this.fileSystem.listStatus(new Path(sourceFile))).filter(item -> item.isDirectory() && !item.getPath().getName().startsWith(".")).map(item -> item.getPath().getName()).collect(Collectors.toList());
            return dirNames.contains("manifest") && dirNames.contains("schema") && dirNames.contains("snapshot") && dirNames.size() >= 4;
        }

        @Override
        public void startRead(ChannelOutput channelOutput) {
            LOG.warn("paimon format not test yet");
            super.startRead(channelOutput);
        }
    }

}