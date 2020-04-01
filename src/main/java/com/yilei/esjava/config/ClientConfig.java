package com.zlst.uimp.cloud.ccapi.module.log_search.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;

/**
 * @Auther: yilei
 * @Date: 2020/3/31
 * @Description:
 */
@Configuration
public class ClientConfig {

    @Value("${elasticSearch.ips}")
    private String hostNames;

    @Value("${elasticSearch.port}")
    private Integer port;

    @Bean
    public TransportClient client() throws Exception {
        TransportClient client = new PreBuiltTransportClient(Settings.EMPTY);
        String[] hosts = hostNames.split(",");
        for (String hostname : hosts) {
            client.addTransportAddress(new TransportAddress(InetAddress.getByName(hostname), port));
        }
        return client;
    }
}
