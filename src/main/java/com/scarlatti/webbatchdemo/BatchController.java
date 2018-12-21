package com.scarlatti.webbatchdemo;

import com.scarlatti.webbatchdemo.config.BeanNames;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

/**
 * ______    __                         __           ____             __     __  __  _
 * ___/ _ | / /__ ___ ___ ___ ____  ___/ /______    / __/______ _____/ /__ _/ /_/ /_(_)
 * __/ __ |/ / -_|_-<(_-</ _ `/ _ \/ _  / __/ _ \  _\ \/ __/ _ `/ __/ / _ `/ __/ __/ /
 * /_/ |_/_/\__/___/___/\_,_/_//_/\_,_/_/  \___/ /___/\__/\_,_/_/ /_/\_,_/\__/\__/_/
 * Monday, 12/17/2018
 */
@RestController
@RequestMapping("/batch")
public class BatchController {

    private JobLauncher synchronousJobLauncher;
    private JobLauncher asyncJobLauncher;
    private Job greetingJob;
    private JobOperator jobOperator;

    public BatchController(@Qualifier("jobLauncher") JobLauncher synchronousJobLauncher,
                           @Qualifier(BeanNames.AsyncJobLauncher) JobLauncher asyncJobLauncher,
                           @Qualifier(BeanNames.GreetingJob) Job greetingJob,
                           JobOperator jobOperator) {
        this.synchronousJobLauncher = synchronousJobLauncher;
        this.asyncJobLauncher = asyncJobLauncher;
        this.greetingJob = greetingJob;
        this.jobOperator = jobOperator;
    }

    @GetMapping("greeting")
    public ResponseEntity<String> greetingBatch(@RequestParam(value = "async", required = false) String async,
                                                @RequestParam(value = "i", required = false) Long i) {
        if (async == null) {
            return launchSynchronous();
        } else {
            return launchAsynchronous(i);
        }
    }

    @GetMapping("stop")
    public ResponseEntity<String> stop(@RequestParam("id") String id) {
        try {
            boolean stopped = jobOperator.stop(Long.valueOf(id));
            if (stopped) {
                return ResponseEntity.ok("stopped.");
            } else {
                throw new RuntimeException("Unable to stop job");
            }
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    private ResponseEntity<String> launchSynchronous() {
        JobParameters params = new JobParametersBuilder()
            .addLong("startTime", ZonedDateTime.now().truncatedTo(ChronoUnit.MINUTES).toEpochSecond())
            .toJobParameters();

        try {
            JobExecution jobExecution = synchronousJobLauncher.run(greetingJob, params);
            return ResponseEntity.ok(jobExecution.getExitStatus().getExitDescription());
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    private ResponseEntity<String> launchAsynchronous(Long i) {
        JobParameters params = new JobParametersBuilder()
            .addLong("startTime", ZonedDateTime.now().truncatedTo(ChronoUnit.MINUTES).toEpochSecond())
            .addLong("count", i)
            .toJobParameters();

        try {
            JobExecution jobExecution = asyncJobLauncher.run(greetingJob, params);
            return ResponseEntity.ok(String.valueOf(jobExecution.getJobId()));
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }
}
