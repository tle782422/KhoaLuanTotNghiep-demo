package com.test.hadoop.modules.config;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HadoopConfig {
    @Bean
    public FileSystem initFileSystem() throws Exception {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.setBoolean("dfs.client.use.datanode.hostname", true);
        conf.setBoolean("dfs.datanode.use.datanode.hostname", true);
        conf.set("fs.defaultFS", "hdfs://namenode:9000");
        conf.set("dfs.replication", "3");
        conf.set("dfs.blocksize", "64m");
        UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("root"));
        return FileSystem.get(conf);
    }
}
