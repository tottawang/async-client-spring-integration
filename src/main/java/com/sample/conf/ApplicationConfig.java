package com.sample.conf;

import java.util.concurrent.Executor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.ExecutorChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.util.CallerBlocksPolicy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.sample.service.Aggregator;
import com.sample.service.Cleaner;
import com.sample.service.Handler;
import com.sample.service.Splitter;
import com.sample.service.Transformer;

@Configuration
public class ApplicationConfig {

  @Autowired
  private Transformer transformer;

  @Autowired
  private Splitter splitter;

  @Autowired
  private Handler handler;

  @Autowired
  private Cleaner cleaner;

  @Autowired
  private Aggregator aggregator;

  @Bean(name = "primaryWorkers")
  public Executor executors() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    int poolSize = 128;
    executor.setCorePoolSize(8);
    executor.setMaxPoolSize(poolSize);
    executor.setQueueCapacity(256);
    executor.setThreadNamePrefix("PrimaryWorkerThread-");
    executor.setRejectedExecutionHandler(new CallerBlocksPolicy(5000));
    executor.initialize();
    return executor;
  }

  @Bean
  @Qualifier("primaryWorkerChannel")
  public ExecutorChannel workerChannel() {
    return MessageChannels.executor(executors()).get();
  }

  @Bean
  @Qualifier("aggregatorChannel")
  public DirectChannel aggregatorChannel() {
    return MessageChannels.direct().get();
  }

  @Bean
  public IntegrationFlow primaryFlow() {
    return IntegrationFlows.from(workerChannel()).transform(transformer).split(splitter)
        .handle(m -> handler.getMessage(m)).get();
  }

  @Bean
  public IntegrationFlow aggregatorFlow() {
    return IntegrationFlows.from(aggregatorChannel())
        .aggregate(a -> a.processor(aggregator).groupTimeout(120000))
        .handle(m -> cleaner.cleanup(m)).get();
  }
}
