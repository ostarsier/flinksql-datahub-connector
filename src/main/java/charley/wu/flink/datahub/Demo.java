package charley.wu.flink.datahub;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.example.examples.Constant;
import com.aliyun.datahub.client.exception.*;
import com.aliyun.datahub.client.http.HttpConfig;
import com.aliyun.datahub.client.model.*;

import java.math.BigDecimal;
import java.util.*;

/**
 * @Author: yangxianbin
 * @Date: 2022/3/15 15:08
 */
public class Demo {

    static DatahubClient datahubClient;

    public static void main(String[] args) {

        // Endpoint以Region: 华东1为例，其他Region请按实际情况填写
        String endpoint = "https://dh-cn-hangzhou.aliyuncs.com";
        String accessId = "";
        String accessKey = "";
        // 创建DataHubClient实例
        datahubClient = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(
                        new DatahubConfig(endpoint,
                                // 是否开启二进制传输，服务端2.12版本开始支持
                                new AliyunAccount(accessId, accessKey), true))
                //专有云使用出错尝试将参数设置为           false
                // HttpConfig可不设置，不设置时采用默认值
                .setHttpConfig(new HttpConfig()
                        .setCompressType(CompressType.LZ4) // 读写数据推荐打开网络传输 LZ4压缩
                        .setConnTimeout(10000))
                .build();

        String project = "";
        String topic = "";
        int retryTimes = 3;

//        listProject();
        tupleExample(project, topic, retryTimes);

//        String subId = "";
//        example(project, topic, subId);

    }


    // 写入Tuple型数据
    public static void tupleExample(String project, String topic, int retryTimes) {
        // 获取schema
        RecordSchema recordSchema = datahubClient.getTopic(project, topic).getRecordSchema();
        // 生成十条数据
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 10; i < 20; ++i) {
            RecordEntry recordEntry = new RecordEntry();
            // 对每条数据设置额外属性，例如ip 机器名等。可以不设置额外属性，不影响数据写入
            recordEntry.addAttribute("key1", "value1");

            TupleRecordData data = new TupleRecordData(recordSchema);
            data.setField("id",  i);
            data.setField("name", "name~" + i);
            data.setField("isAdult", true);
            data.setField("gender", 1);
            data.setField("isForeigner", 1);
            data.setField("seq", 123456);
            data.setField("money", 1.34);
            data.setField("height", 45.54);
            data.setField("ts", 1647436755000L);
            recordEntry.setRecordData(data);
            recordEntries.add(recordEntry);
        }
        try {
            PutRecordsResult result = datahubClient.putRecords(project, topic, recordEntries);
            int i = result.getFailedRecordCount();
            if (i > 0) {
                retry(datahubClient, result.getFailedRecords(), retryTimes, project, topic);
            }
        } catch (DatahubClientException e) {
            System.out.println("requestId:" + e.getRequestId() + "\tmessage:" + e.getErrorMessage());
        }
    }

    //重试机制
    public static void retry(DatahubClient client, List<RecordEntry> records, int retryTimes, String project, String topic) {
        boolean suc = false;
        while (retryTimes != 0) {
            retryTimes = retryTimes - 1;
            PutRecordsResult recordsResult = client.putRecords(project, topic, records);
            if (recordsResult.getFailedRecordCount() > 0) {
                retry(client, recordsResult.getFailedRecords(), retryTimes, project, topic);
            }
            suc = true;
            break;
        }
        if (!suc) {
            System.out.println("retryFailure");
        }
    }


    //
    //点位消费示例，并在消费过程中进行点位的提交
    public static void example(String project, String topic, String subId) {
        String shardId = "0";
        List<String> shardIds = Arrays.asList("0", shardId);

        OpenSubscriptionSessionResult openSubscriptionSessionResult = datahubClient.openSubscriptionSession(project, topic, subId, shardIds);
        SubscriptionOffset subscriptionOffset = openSubscriptionSessionResult.getOffsets().get(shardId);
        // 1、获取当前点位的cursor，如果当前点位已过期则获取生命周期内第一条record的cursor，未消费同样获取生命周期内第一条record的cursor
        String cursor = null;
        //sequence < 0说明未消费
        if (subscriptionOffset.getSequence() < 0) {
            // 获取生命周期内第一条record的cursor
            cursor = datahubClient.getCursor(project, topic, shardId, CursorType.OLDEST).getCursor();
        } else {
            // 获取下一条记录的Cursor
            long nextSequence = subscriptionOffset.getSequence() + 1;
            try {
                //按照SEQUENCE getCursor可能报SeekOutOfRange错误，表示当前cursor的数据已过期
                cursor = datahubClient.getCursor(project, topic, shardId, CursorType.SEQUENCE, nextSequence).getCursor();
            } catch (SeekOutOfRangeException e) {
                // 获取生命周期内第一条record的cursor
                cursor = datahubClient.getCursor(project, topic, shardId, CursorType.OLDEST).getCursor();
            }
        }
        // 2、读取并保存点位，这里以读取Tuple数据为例，并且每1000条记录保存一次点位
        long recordCount = 0L;
        // 每次读取10条record
        int fetchNum = 10;
        while (true) {
            try {
                RecordSchema schema = datahubClient.getTopic(project, topic).getRecordSchema();

                GetRecordsResult getRecordsResult = datahubClient.getRecords(project, topic, shardId, schema, cursor, fetchNum);
                if (getRecordsResult.getRecordCount() <= 0) {
                    // 无数据，sleep后读取
                    Thread.sleep(1000);
                    continue;
                }
                for (RecordEntry recordEntry : getRecordsResult.getRecords()) {
                    //消费数据
                    TupleRecordData data = (TupleRecordData) recordEntry.getRecordData();
                    System.out.println("id:" + data.getField("id") + "\t"
                            + "name:" + data.getField("name"));
                    // 处理数据完成后，设置点位
                    ++recordCount;
                    subscriptionOffset.setSequence(recordEntry.getSequence());
                    subscriptionOffset.setTimestamp(recordEntry.getSystemTime());
                    if (recordCount % 1000 == 0) {
                        //提交点位点位
                        Map<String, SubscriptionOffset> offsetMap = new HashMap<>();
                        offsetMap.put(shardId, subscriptionOffset);
                        datahubClient.commitSubscriptionOffset(Constant.projectName, Constant.topicName, subId, offsetMap);
                        System.out.println("commit offset successful");
                    }
                }
                cursor = getRecordsResult.getNextCursor();
            } catch (SubscriptionOfflineException | SubscriptionSessionInvalidException e) {
                // 退出. Offline: 订阅下线; SubscriptionSessionInvalid: 表示订阅被其他客户端同时消费
                break;
            } catch (SubscriptionOffsetResetException e) {
                // 表示点位被重置，重新获取SubscriptionOffset信息，这里以Sequence重置为例
                // 如果以Timestamp重置，需要通过CursorType.SYSTEM_TIME获取cursor
                subscriptionOffset = datahubClient.getSubscriptionOffset(Constant.projectName, Constant.topicName, subId, shardIds).getOffsets().get(shardId);
                long nextSequence = subscriptionOffset.getSequence() + 1;
                cursor = datahubClient.getCursor(Constant.projectName, Constant.topicName, shardId, CursorType.SEQUENCE, nextSequence).getCursor();
            } catch (DatahubClientException e) {
                // TODO: 针对不同异常决定是否退出
            } catch (Exception e) {
                break;
            }
        }
    }


    public static void listProject() {
        try {
            ListProjectResult listProjectResult = datahubClient.listProject();
            if (listProjectResult.getProjectNames().size() > 0) {
                for (String pName : listProjectResult.getProjectNames()) {
                    System.out.println(pName);
                }
            }
        } catch (DatahubClientException e) {
            System.out.println(e.getErrorMessage());
        }
    }

}
