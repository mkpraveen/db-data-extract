package org.praveenmk.batch.learn.dbdataextractor.config;


import org.praveenmk.batch.learn.dbdataextractor.model.UserDetails;
import org.praveenmk.batch.learn.dbdataextractor.processor.UserDetailsItemProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

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
    DataSource dataSource;


    @Bean
    public DataSource dataSource() {
        final DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://localhost:3306/movie_rating?useSSL=false");
        dataSource.setUsername("movieadmin");
        dataSource.setPassword("movieadmin123");

        return dataSource;
    }

    @Bean
    public JdbcCursorItemReader<UserDetails> reader() {
        JdbcCursorItemReader<UserDetails> itemReader = new JdbcCursorItemReader<>();
        itemReader.setDataSource(dataSource);
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
        final Step step1 = stepBuilderFactory.get("step1")
                .<UserDetails,UserDetails>chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer()).build();
        return step1;
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
