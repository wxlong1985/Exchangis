package com.alibaba.datax.plugin.writer.hdfswriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.collect.Lists;
import com.webank.wedatasphere.exchangis.datax.core.transport.stream.ChannelInput;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.utils.HadoopUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class HdfsWithPaimonWriter extends Writer {


    /**
     * just check the folder struct
     *
     * @return
     * @throws IOException
     */
    static boolean isPaimonDir(FileSystem fileSystem, String sourceFile) throws IOException {
        List<String> dirNames = Arrays.stream(fileSystem.listStatus(new Path(sourceFile))).filter(item -> item.isDirectory() && !item.getPath().getName().startsWith(".")).map(item -> item.getPath().getName()).collect(Collectors.toList());
        return dirNames.contains("manifest") && dirNames.contains("schema") && dirNames.contains("snapshot") && dirNames.size() >= 4;
    }


    static HiveCatalog catalog(Configuration configuration) throws IOException {
        String warehouse = new Path(configuration.getString("defaultFS") + configuration.getString("path")).getParent().getParent().toString();
        HiveConf hiveConf =
                HiveCatalog.createHiveConf(
                        System.getProperty("HIVE_CONF_DIR"), null, HadoopUtils.getHadoopConfiguration(new Options()));
        String metastoreClientClass = HiveMetaStoreClient.class.getName();
        Options catalogOptions = new Options();
        catalogOptions.set(CatalogOptions.WAREHOUSE, warehouse);
        CatalogContext catalogContext = CatalogContext.create(catalogOptions);
        FileIO fileIO = FileIO.get(new org.apache.paimon.fs.Path(warehouse), catalogContext);
        return new HiveCatalog(fileIO, hiveConf, metastoreClientClass, warehouse);
    }

    static Table table(HiveCatalog hiveCatalog, Configuration configuration) throws Exception {
        Identifier tableIdentifier = Identifier.create(configuration.getString("hiveDatabase"), configuration.getString("hiveTable"));
        return hiveCatalog.getTable(tableIdentifier);
    }

    static Table table(Configuration configuration) throws Exception {
        HiveCatalog hiveCatalog = catalog(configuration);
        Identifier tableIdentifier = Identifier.create(configuration.getString("hiveDatabase"), configuration.getString("hiveTable"));
        return hiveCatalog.getTable(tableIdentifier);
    }

    public static class Job extends HdfsWriter.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        Configuration writerSliceConfig;

        HdfsWriterUtil writerUtil;

        @Override
        public void init() {
            super.init();
            this.writerSliceConfig = this.getPluginJobConf();
            try {
                this.writerUtil = (HdfsWriterUtil) FieldUtils.readField(this, "hdfsWriterUtil", true);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }

        }

        @Override
        public void prepare() {
            String path = writerSliceConfig.getString("path");
            try {
                if (isPaimonDir(writerUtil.fileSystem, path)) {
                    HiveCatalog catalog = catalog(writerSliceConfig);

                    String writeMode = this.writerSliceConfig.getNecessaryValue(Key.WRITE_MODE, HdfsWriterErrorCode.REQUIRED_VALUE);
                    writeMode = writeMode.toLowerCase().trim();
                    if ("truncate".equals(writeMode)) {
                        LOG.info(String.format("由于您配置了writeMode truncate, 开始清理 [%s] 下面的内容", path));
                        Identifier tableIdentifier = Identifier.create(writerSliceConfig.getString("hiveDatabase"), writerSliceConfig.getString("hiveTable"));
                        org.apache.paimon.table.Table table = catalog.getTable(tableIdentifier);
                        TableSchema tableSchema = ((FileStoreTable) table).schema();
                        Schema schema = new Schema(tableSchema.fields(), tableSchema.partitionKeys(), tableSchema.primaryKeys(), tableSchema.options(), tableSchema.comment());
                        catalog.dropTable(tableIdentifier, true);
                        catalog.createTable(tableIdentifier, schema, true);
                    }
                } else {
                    super.prepare();
                }


            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                throw DataXException.asDataXException(HdfsWriterErrorCode.ILLEGAL_VALUE, String.format("prepare path %s ", path));
            }

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            return Lists.newArrayList(this.getPluginJobConf().clone());
        }
    }

    public static class Task extends HdfsWriter.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        Configuration writerSliceConfig;

        HdfsWriterUtil writerUtil;
        String path;

        @Override
        public void init() {
            super.init();
            this.writerSliceConfig = this.getPluginJobConf();
            try {
                this.writerUtil = (HdfsWriterUtil) FieldUtils.readField(this, "hdfsWriterUtil", true);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            this.path = this.writerSliceConfig.getNecessaryValue(Key.PATH, HdfsWriterErrorCode.REQUIRED_VALUE);
        }

        @Override
        public void prepare() {

        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            try {
                if (isPaimonDir(this.writerUtil.fileSystem, path)) {
                    Record record = null;
                    Table table = table(this.writerSliceConfig);
                    BatchWriteBuilder batchWriteBuilder = table.newBatchWriteBuilder();
                    BatchTableWrite batchTableWrite = batchWriteBuilder.newWrite();
                    batchTableWrite.withIOManager(IOManager.create(FileUtils.getTempDirectoryPath()));
                    int i = 1;
                    List<Configuration> columns = this.writerSliceConfig.getListConfiguration(Key.COLUMN);
                    while ((record = lineReceiver.getFromReader()) != null) {
                        GenericRow row = new GenericRow(record.getColumns().size());
                        for (int index = 0; index < columns.size(); index++) {
                            Column column = record.getColumns().get(index);
                            String rowData = column.getRawData().toString();
                            SupportHiveDataType columnType = SupportHiveDataType.valueOf(column.getType().name().toUpperCase());
                            try {
                                switch (columnType) {
                                    case TINYINT:
                                        row.setField(index, Byte.valueOf(rowData));
                                        break;
                                    case SMALLINT:
                                        row.setField(index, Short.valueOf(rowData));
                                        break;
                                    case INT:
                                        row.setField(index, Integer.valueOf(rowData));
                                        break;
                                    case BIGINT:
                                    case LONG:
                                        row.setField(index, column.asLong());
                                        break;
                                    case FLOAT:
                                        row.setField(index, Float.valueOf(rowData));
                                        break;
                                    case DOUBLE:
                                        row.setField(index, column.asDouble());
                                        break;
                                    case STRING:
                                    case VARCHAR:
                                    case ARRAY:
                                    case MAP:
                                    case CHAR:
                                        row.setField(index, BinaryString.fromString(column.asString()));
                                        break;
                                    case BOOLEAN:
                                        row.setField(index, column.asBoolean());
                                        break;
                                    case DATE:
                                        row.setField(index, new java.sql.Date(column.asDate().getTime()));
                                        break;
                                    case TIMESTAMP:
                                        row.setField(index, new java.sql.Timestamp(column.asDate().getTime()));
                                        break;
                                    default:
                                        throw DataXException
                                                .asDataXException(
                                                        HdfsWriterErrorCode.ILLEGAL_VALUE,
                                                        String.format(
                                                                "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
                                                                columns.get(index).getString(Key.NAME),
                                                                columns.get(index).getString(Key.TYPE)));
                                }
                            } catch (Exception e) {
                                // warn: 此处认为脏数据
                                String message = String.format(
                                        "字段类型转换错误：你目标字段为[%s]类型，实际字段值为[%s].",
                                        columns.get(index).getString(Key.TYPE), column.getRawData().toString());
                                getTaskPluginCollector().collectDirtyRecord(record, message);
                                break;
                            }

                        }
                        batchTableWrite.write(row);

                        if ((i++) % 200 == 0) {
                            List<CommitMessage> messages = batchTableWrite.prepareCommit();
                            BatchTableCommit commit = batchWriteBuilder.newCommit();
                            commit.commit(messages);
                        }
                    }
                    List<CommitMessage> commitMessages = batchTableWrite.prepareCommit();
                    if (!commitMessages.isEmpty()) {
                        BatchTableCommit commit = batchWriteBuilder.newCommit();
                        commit.commit(commitMessages);
                    }
                } else {
                    super.startWrite(lineReceiver);
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }


        @Override
        public void startWrite(ChannelInput channelInput) {
            super.startWrite(channelInput);
        }

    }

}
