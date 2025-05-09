package com.algobrewery.tasksilo.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.util.Properties;

@Configuration
@EnableTransactionManagement
@EnableJpaRepositories(basePackages = "com.algobrewery.tasksilo.repository")
@PropertySource("classpath:application.yml")
public class DatabaseConfig {

    @Autowired
    private Environment env;

    @Bean
    public DataSource dataSource() {
        HikariDataSource dataSource = new HikariDataSource();

        dataSource.setDriverClassName(env.getProperty("spring.datasource.driver-class-name"));
        dataSource.setJdbcUrl(env.getProperty("spring.datasource.url"));
        dataSource.setUsername(env.getProperty("spring.datasource.username"));
        dataSource.setPassword(env.getProperty("spring.datasource.password"));

        // HikariCP specific settings
        dataSource.setMaximumPoolSize(Integer.parseInt(env.getProperty("spring.datasource.hikari.maximum-pool-size", "10")));
        dataSource.setMinimumIdle(Integer.parseInt(env.getProperty("spring.datasource.hikari.minimum-idle", "5")));
        dataSource.setIdleTimeout(Long.parseLong(env.getProperty("spring.datasource.hikari.idle-timeout", "300000")));
        dataSource.setConnectionTimeout(Long.parseLong(env.getProperty("spring.datasource.hikari.connection-timeout", "20000")));
        dataSource.setMaxLifetime(Long.parseLong(env.getProperty("spring.datasource.hikari.max-lifetime", "1800000")));
        dataSource.setConnectionTestQuery(env.getProperty("spring.datasource.hikari.connection-test-query", "SELECT 1"));
        dataSource.setValidationTimeout(Long.parseLong(env.getProperty("spring.datasource.hikari.validation-timeout", "3000")));

        return dataSource;
    }

    @Bean
    public LocalContainerEntityManagerFactoryBean entityManagerFactory() {
        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
        em.setDataSource(dataSource());
        em.setPackagesToScan("com.algobrewery.tasksilo.model");

        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        vendorAdapter.setGenerateDdl(false);
        em.setJpaVendorAdapter(vendorAdapter);

        Properties hibernateProperties = new Properties();
        hibernateProperties.setProperty("hibernate.dialect", env.getProperty("spring.jpa.properties.hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect"));
        hibernateProperties.setProperty("hibernate.format_sql", env.getProperty("spring.jpa.properties.hibernate.format_sql", "true"));
        hibernateProperties.setProperty("hibernate.default_schema", env.getProperty("spring.jpa.properties.hibernate.default_schema", "public"));

        // Performance settings
        hibernateProperties.setProperty("hibernate.jdbc.batch_size", env.getProperty("spring.jpa.properties.hibernate.jdbc.batch_size", "20"));
        hibernateProperties.setProperty("hibernate.order_inserts", env.getProperty("spring.jpa.properties.hibernate.order_inserts", "true"));
        hibernateProperties.setProperty("hibernate.order_updates", env.getProperty("spring.jpa.properties.hibernate.order_updates", "true"));
        hibernateProperties.setProperty("hibernate.jdbc.batch_versioned_data", env.getProperty("spring.jpa.properties.hibernate.jdbc.batch_versioned_data", "true"));

        // Cache settings
        hibernateProperties.setProperty("hibernate.generate_statistics", env.getProperty("spring.jpa.properties.hibernate.generate_statistics", "false"));
        hibernateProperties.setProperty("hibernate.cache.use_second_level_cache", env.getProperty("spring.jpa.properties.hibernate.cache.use_second_level_cache", "false"));

        em.setJpaProperties(hibernateProperties);

        return em;
    }

    @Bean
    public PlatformTransactionManager transactionManager() {
        JpaTransactionManager transactionManager = new JpaTransactionManager();
        transactionManager.setEntityManagerFactory(entityManagerFactory().getObject());
        return transactionManager;
    }
}
