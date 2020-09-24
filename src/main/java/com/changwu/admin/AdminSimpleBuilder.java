package com.changwu.admin;

import com.sun.media.jfxmediaimpl.HostUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;

import java.rmi.MarshalledObject;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.jar.JarOutputStream;

public class AdminSimpleBuilder {

    // 构建AdminClient
    public AdminClient buildAdminClient() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        // 设置kafka的地址
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // 请求超时时间
        properties.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "40001");
        AdminClient adminClient = AdminClient.create(properties);
        System.out.println("-----------------");
        adminClient.listTopics().names().get().forEach(System.out::println);
        System.out.println("-----------------");

        return adminClient;
    }

    // 创建Topic
    public void createTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = buildAdminClient();
        // topic的副本数
        short s = 1;
        // 指定Topic的名字，分区数、副本数
        NewTopic topic = new NewTopic("Order22", 1, s);
        // 创建topic的返回值为：CreateTopicsResult
        CreateTopicsResult topics = adminClient.createTopics(Collections.singletonList(topic));
    }

    // 查找Topics
    public void findTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = buildAdminClient();
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        System.out.println("===============");
        listTopicsResult.names().get().forEach(System.out::println);
        System.out.println("===============");
    }

    // 查找Topic
    public void findTopics2() throws ExecutionException, InterruptedException {
        AdminClient adminClient = buildAdminClient();
        ListTopicsOptions options = new ListTopicsOptions();
        options.listInternal(true);
        ListTopicsResult listTopicsResult = adminClient.listTopics(options);
    }

    // 删除Topic
    public void deleteTopic() throws ExecutionException, InterruptedException {
        AdminClient client = buildAdminClient();
        DeleteTopicsResult result = client.deleteTopics(Arrays.asList("Order11"));
    }

    // 查看Topic的描述信息
    public void descTopic() throws ExecutionException, InterruptedException {
        AdminClient client = buildAdminClient();
        Map<String, TopicDescription> descriptionMap = client.describeTopics(Arrays.asList("Order22")).all().get();

        descriptionMap.forEach((k, v) -> {
            System.out.println("topicName=" + k + "  desc=" + v);
        });
    }

    // 查看topic的配置信息
    public void descConfig() throws ExecutionException, InterruptedException {
        AdminClient client = buildAdminClient();
        // ConfigResource configResource1 = new ConfigResource(ConfigResource.Type.BROKER,"xxx");
        // ConfigResource configResource2 = new ConfigResource(ConfigResource.Type.BROKER_LOGGER,"xxx");
        ConfigResource configResource3 = new ConfigResource(ConfigResource.Type.TOPIC,"Order22");

        Map<ConfigResource, Config> configResourceConfigMap = client.describeConfigs(Arrays.asList(configResource3)).all().get();
        configResourceConfigMap.forEach((k,v)->{
            System.out.println("configName=" + k + "  desc=" + v);
            System.out.println("==========================");
        });
    }

    // 修改Topic的配置
    public void alterConfig() throws ExecutionException, InterruptedException {
        AdminClient client = buildAdminClient();
        Map<ConfigResource,Config> configResourceConfigMap = new HashMap<>();
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC,"Order22");
        Config config = new Config(Arrays.asList(new ConfigEntry("preallocate","false")));
        configResourceConfigMap.put(configResource,config);
        AlterConfigsResult alterConfigsResult =  client.alterConfigs(configResourceConfigMap);
    }

    // 修改topic的配置2
    public void alterConfig2() throws ExecutionException, InterruptedException {
        AdminClient client = buildAdminClient();
        Map<ConfigResource, Collection<AlterConfigOp>> configResourceConfigMap = new HashMap<>();

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC,"Order22");
        AlterConfigOp alterConfigOp = new AlterConfigOp(new ConfigEntry("preallocate", "true"),
                                                                    AlterConfigOp.OpType.SET);
        configResourceConfigMap.put(configResource,Arrays.asList(alterConfigOp));
        AlterConfigsResult alterConfigsResult =  client.incrementalAlterConfigs(configResourceConfigMap);
    }

    // 创建partition
    public void cPartion() throws ExecutionException, InterruptedException {
        AdminClient client = buildAdminClient();
        Map<String, NewPartitions> map = new HashMap<>();
        NewPartitions newPartitions1 = NewPartitions.increaseTo(2);
        map.put("Order22",newPartitions1);
        client.createPartitions(map).all().get();
    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        AdminSimpleBuilder adminSimpleBuilder = new AdminSimpleBuilder();
        //adminSimpleBuilder.createTopics();
        //adminSimpleBuilder.findTopics();
        adminSimpleBuilder.cPartion();
        adminSimpleBuilder.descTopic();
    }
}
