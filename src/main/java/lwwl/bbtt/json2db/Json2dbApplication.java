package lwwl.bbtt.json2db;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Configuration
@EnableBatchProcessing
@SpringBootApplication
public class Json2dbApplication {

    private static final Logger logger = LoggerFactory.getLogger(Json2dbApplication.class);

    @Value("${cfg.filePath}")
    private String filePath;
    @Value("${cfg.initId}")
    private int initId;
    /*执行时间*/
    private Date now = new Date();

    @Autowired
    private JobBuilderFactory jobs;
    @Autowired
    private StepBuilderFactory steps;
    @Autowired
    private DataSource dataSource;

    private AtomicInteger idIncrementer = new AtomicInteger(0);

    public static void main(String[] args) {
//        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder().addString("inputFile", args[0]);

        System.exit(SpringApplication
                .exit(SpringApplication.run(Json2dbApplication.class, args)));
    }

    @Bean
    public Job job1(Step step) {
        return jobs.get("job1")
                .incrementer(new RunIdIncrementer())
                .start(step)
//                .next(reportDecider)
//                .on("SEND").to(sendReportStep)
//                .on("SKIP").end().build()
                .build();
    }

    @Bean
    public Step step1(ItemReader<JSONObject> reader, JobExecutionListener listener) {
        return steps.get("step1")
//                .tasklet(tasklet())
                .<JSONObject, JSONObject>chunk(1)
                .reader(reader)
				.processor(processor())
                .writer(writer())
                .listener(listener)
                .faultTolerant()
                .skipLimit(10)
				.skip(Exception.class)
//				.skip(ServiceUnavailableException.class)
//				.retryLimit(5)
//				.retry(ServiceUnavailableException.class)
//				.backOffPolicy(backoffPolicy)
//				.listener(logSkipListener())
//                .taskExecutor(new SimpleAsyncTaskExecutor())
//                .throttleLimit(10)
                .build();
    }


//    @Bean
//    protected Tasklet tasklet() {
//        return (contribution, context) -> RepeatStatus.FINISHED;
//    }

    @Bean
    @StepScope
    public ItemReader<JSONObject> reader() throws IOException {
        FileSystemResource fileSystemResource = new FileSystemResource(filePath);
        logger.info("filePath:" + fileSystemResource.getURL().getPath());
        FlatFileItemReader<JSONObject> reader = new FlatFileItemReader<JSONObject>();
        reader.setResource(fileSystemResource);
        reader.setLineMapper(new JsonLineMapper());
        reader.setStrict(false);
        reader.open(new ExecutionContext());

        return reader;
    }


    /**
     * 处理过程
     *
     * @return
     */
    @Bean
    @StepScope
    public ItemProcessor<JSONObject, JSONObject> processor() {
//        @Value("#{jobParameters[inputFileBlack]}") String inputFile

        return item -> {
//            Map<String, Object> item = new HashMap<>();
//            item.put("sourceid", item.get("sourceid"));
//            item.put("sourcetype", item.get("sourcetype"));
//            item.put("sourcedoc", item.get("sourcedoc"));

            int id = idIncrementer.getAndIncrement() + initId;
            item.put("id", id);
            item.put("bizcode", item.getOrDefault(  "sourceid",id ));
            item.put("bizname", item.get("案件名称"));
            item.put("bizdate", item.get("裁判日期"));
            item.put("level", item.get("审判程序"));
            Object judgetype = item.get("案件类型");
            item.put("judgetype", "1".equals(judgetype) ? "judgetypexs" : judgetype);
            item.put("wh", item.get("案号"));
            item.put("org", item.get("法院名称"));
            String docContent = item.get("DocContent").toString();
            item.put("doc", tranDoc(docContent));
            //docContent 中 "判决如下：" 到 "如不服本判决" 之间内容
            item.put("result", extractResult(docContent));
            item.put("fair", item.get("裁判要旨段原文"));

            item.put("state", item.get("已发布"));
            //audituser, :auditat, :createuser, :createat, :updateuser, :updateat
            String userid = "8";//管理员
            item.put("audituser", userid);
            item.put("auditat", now);
            item.put("createuser", userid);
            item.put("createat", now);
            item.put("updateuser", userid);
            item.put("updateat", now);
            item.put("publishuser", userid);
            item.put("publishat", now);

            return item;
        };
    }

    private String tranDoc(String docContent) {
        //换行之间的修改增加段落标记方便显示
        String doc = Arrays.stream(docContent.split("\\n")).map((string) -> "<p>" + string + "</p>").reduce((str1, str2) -> str1 + str2).get();
        return doc;
    }

    //docContent 中 "判决如下：" 到 "如不服本判决" 之间内容
    private Pattern resultPattern = Pattern.compile("判决如下：(.*)如不服本判决", Pattern.MULTILINE|Pattern.DOTALL);
    private String extractResult(String docContent) {
        Matcher matcher = resultPattern.matcher(docContent);
        if (matcher.find()) {
            String result = matcher.group(1);
            return result;
        }
        return null;
    }

//	@Bean
//	@StepScope
//	public LineMapper<BlackListDO> lineMapper() {
//		DefaultLineMapper<BlackListDO> lineMapper = new DefaultLineMapper<BlackListDO>();
//		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
//		lineTokenizer.setDelimiter(",");
//		lineTokenizer.setStrict(false);
//		lineTokenizer.setNames(new String[] { "type","value","fraudType"});
//
//		BeanWrapperFieldSetMapper<BlackListDO> fieldSetMapper = new BeanWrapperFieldSetMapper<BlackListDO>();
//		fieldSetMapper.setTargetType(BlackListDO.class);
//		lineMapper.setLineTokenizer(lineTokenizer);
//		lineMapper.setFieldSetMapper(new BlackListFieldSetMapper());
//		return lineMapper;
//	}


    @Bean
    @StepScope
    public ItemWriter<JSONObject> writer() {
        JdbcBatchItemWriter<JSONObject> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setAssertUpdates(true);
        writer.setSql("INSERT INTO `law`.`law_judge2` (`id`, `bizcode`, `bizname`, `bizdate`, `status`, `audituser`, `auditat`, `createuser`, `createat`, `updateuser`, `updateat`, `reason`, `fair`, `judgetype`, `org`, `level`, `result`, `wh`, `doc`, `publishat`, `publishuser`, sourceid, sourcetype, sourcedoc) " +
                "VALUES (:id, :bizcode, :bizname, :bizdate, 'status', :audituser, :auditat, :createuser, :createat, :updateuser, :updateat, :reason, :fair, :judgetype, :org, :level, :result, :wh, :doc, :publishat, :publishuser, :sourceid, :sourcetype, :sourcedoc)");
        return writer;
    }
}
