package com.streamlite.broker.timescale.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class DatastoreConfiguration {

    @Value("${main.datasource.host}")
    private String host;
    @Value("${main.datasource.port}")
    private String port;
    @Value("${main.datasource.database}")
    private String database;
    @Value("${main.datasource.username}")
    private String username;
    @Value("${main.datasource.password}")
    private String password;
    @Value("${main.datasource.driver-class-name}")
    private String driver;

    @Bean("mainDataSource")
    public DataSource mainDataSource() {

        String url = "jdbc:postgresql://" + host + ":" + port + "/" + database;

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);
        config.setDriverClassName(driver);

        // Optional tuning
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setIdleTimeout(30000);
        config.setConnectionTimeout(30000);

        return new HikariDataSource(config);
    }

    @Bean("datastoreTemplate")
    public JdbcTemplate datastoreTemplate(
            @Autowired
            @Qualifier("mainDataSource")
            DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

}
