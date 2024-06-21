package com.example.demo.Configuration;

import com.example.demo.BatchProcessing.PersonItemProcessor;
import com.example.demo.DTO.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.boot.autoconfigure.batch.JobLauncherApplicationRunner;

import javax.sql.DataSource;



@Configuration
public class BatchConfiguration {


    @Bean
    public FlatFileItemReader<Person> reader() {
        return new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader")
                .resource(new ClassPathResource("contacts.csv"))
                .delimited()
                .names("id","name", "email")
                .targetType(Person.class)
                .build();
    }
    @Bean
    public PersonItemProcessor processor(){
        return new PersonItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<Person> writer(@Qualifier("postgresqlDataSource") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .sql("INSERT INTO people (person_id,person_name,person_email) VALUES (:id, :name, :email)")
                .dataSource(dataSource)
                .beanMapped()
                .build();
    }

    //JdbcBatchItemWriter para escribir los datos en la base de datos.

    @Bean
    @Order(1)
    public Job importUserJob(JobRepository jobRepository, Step step1ImportUserJob){
        return new JobBuilder("importUserJob", jobRepository)
                .start(step1ImportUserJob)
                .build();
    }

    @Bean
    public Step step1ImportUserJob (JobRepository jobRepository, DataSourceTransactionManager transactionManager,
                                    FlatFileItemReader<Person> reader,PersonItemProcessor processor, JdbcBatchItemWriter<Person> writer) {
        return new StepBuilder("step1ImportUserJob", jobRepository)
                .<Person, Person>chunk(3, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public JdbcCursorItemReader<Person> readDB(@Qualifier("postgresqlDataSource")DataSource dataSource) {
        return new JdbcCursorItemReaderBuilder<Person>()
                .name("personItemReader")
                .dataSource(dataSource)
                .sql("SELECT id, name, email FROM people")
                .rowMapper(new BeanPropertyRowMapper<>(Person.class))
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<Person> writeDB(@Qualifier("h2DataSource")DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO people (id, name, email) VALUES (:id, :name, :email)")
                .dataSource(dataSource)
                .build();
    }
    //segundo Job que utilice un JdbcCursorItemReader para extraer datos de una base de datos fuente.
    @Bean
    @Order(2)
    public Job exportUserJob(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                             DataSource postgresqlDataSource, DataSource h2DataSource) {
        return new JobBuilder("exportUserJob", jobRepository)
                .start(step1ExportUserJob(jobRepository, transactionManager, postgresqlDataSource, h2DataSource))
                .build();
    }

    @Bean
    public Step step1ExportUserJob(JobRepository jobRepository, PlatformTransactionManager transactionManager, DataSource postgresqlDataSource, DataSource h2DataSource) {
        return new StepBuilder("step1ExportUserJob", jobRepository)
                .<Person, Person>chunk(10, transactionManager)
                .reader(readDB(postgresqlDataSource))
                .writer(writeDB(h2DataSource))
                .build();
    }

}
