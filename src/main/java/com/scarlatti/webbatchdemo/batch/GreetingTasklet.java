package com.scarlatti.webbatchdemo.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.StoppableTasklet;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

/**
 * ______    __                         __           ____             __     __  __  _
 * ___/ _ | / /__ ___ ___ ___ ____  ___/ /______    / __/______ _____/ /__ _/ /_/ /_(_)
 * __/ __ |/ / -_|_-<(_-</ _ `/ _ \/ _  / __/ _ \  _\ \/ __/ _ `/ __/ / _ `/ __/ __/ /
 * /_/ |_/_/\__/___/___/\_,_/_//_/\_,_/_/  \___/ /___/\__/\_,_/_/ /_/\_,_/\__/\__/_/
 * Monday, 12/17/2018
 */
@Component
public class GreetingTasklet implements StoppableTasklet {

    private static final Logger log = LoggerFactory.getLogger(GreetingTasklet.class);
    private FutureTask<RepeatStatus> tasklet;
    private ExecutorService taskExecutor = Executors.newSingleThreadExecutor();

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        tasklet = new FutureTask<>(() -> {
            return doExecute(contribution, chunkContext);
        });
        taskExecutor.submit(tasklet);

        return tasklet.get();
    }

    private RepeatStatus doExecute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        Long max = 5L;
        Long count = (Long) chunkContext.getStepContext().getJobParameters().get("count");

        if (count != null) {
            max = count;
        }

        for (long i = 0; i < max; i++) {
            Thread.sleep(1000);
            System.out.println("Hello " + i + "/" + max + "!");
        }

        contribution.setExitStatus(new ExitStatus("done", "Done Greeting :)"));

        return RepeatStatus.FINISHED;
    }

    @Override
    public void stop() {
        log.info("stopping");
        tasklet.cancel(true);
    }
}
