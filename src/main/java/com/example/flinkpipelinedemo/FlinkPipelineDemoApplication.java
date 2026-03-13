package com.example.flinkpipelinedemo;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class FlinkPipelineDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(FlinkPipelineDemoApplication.class, args);
    }
    @Bean
    public ApplicationRunner runner(ApplicationContext context) {
        return args -> {
            String[] beanNames = context.getBeanNamesForType(CommandLineRunner.class);
            for (String beanName : beanNames) {
                System.out.println(beanName + " -> " + context.getBean(beanName).getClass().getName());
            }
        };
    }
}
