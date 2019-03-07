package org.praveenmk.batch.learn.dbdataextractor.config;


import org.praveenmk.batch.learn.dbdataextractor.model.UserDetails;
import org.praveenmk.batch.learn.dbdataextractor.processor.UserDetailsItemProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.*;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

@Configuration
@EnableBatchProcessing
public class DbExtractSpringBootConfig {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    DataSource appDataSource;

    @Autowired
    DataSource batchDataSource;

    @Primary
    @Bean
    @ConfigurationProperties(prefix="spring.app.datasource")
    public DataSource appDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    @ConfigurationProperties(prefix="spring.batchconfig.datasource")
    public DataSource batchDataSource(){
        return DataSourceBuilder.create().build();
    }


    @Bean
    BatchConfigurer configurer(@Qualifier("batchDataSource") DataSource dataSource){
        return new DefaultBatchConfigurer(dataSource);
    }


    @Bean
    public JdbcCursorItemReader<UserDetails> reader() {
        JdbcCursorItemReader<UserDetails> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(appDataSource);
        itemReader.setSql("SELECT * FROM USER_DETAILS");
        itemReader.setRowMapper(new UserDetailsRowMapper());
        return itemReader;
    }

    public class UserDetailsRowMapper implements RowMapper<UserDetails> {

        @Override
        public UserDetails mapRow(ResultSet rs, int rowNum) throws SQLException {
            UserDetails userDetails = new UserDetails();
            userDetails.setUserId(rs.getLong("user_id"));
            userDetails.setUserName(rs.getString("user_name"));
            userDetails.setEmailAddress(rs.getString("email_address"));
            userDetails.setPhoneNumber(rs.getString("phone_number"));
            return userDetails;
        }
    }

    @Bean
    public UserDetailsItemProcessor processor(){
        return new UserDetailsItemProcessor();
    }

    @Bean
    public BeanWrapperFieldExtractor userFieldExtractor (){
        BeanWrapperFieldExtractor<UserDetails> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(new String[] {"userId", "userName","phoneNumber", "emailAddress"});
        return extractor;
    }

    @Bean
    public FlatFileItemWriter<UserDetails> writer() {
        FlatFileItemWriter<UserDetails> fileItemWriter = new FlatFileItemWriter<>();
        fileItemWriter.setResource(new FileSystemResource("c:/temp/user_details.csv"));

        fileItemWriter.setLineAggregator(new DelimitedLineAggregator<UserDetails>() {
            {
             setDelimiter("|");
             setFieldExtractor(userFieldExtractor());
            }
        });
        return fileItemWriter;
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<UserDetails,UserDetails>chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer()).build();
    }


    @Bean
    public Job exportUserJob() {
        return jobBuilderFactory.get("exportUserJob")
                .incrementer(new RunIdIncrementer())
                .flow(step1())
                .end()
                .build();
    }
}
