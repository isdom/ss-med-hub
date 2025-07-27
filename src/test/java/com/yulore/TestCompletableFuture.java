package com.yulore;

import com.yulore.util.ExceptionUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

@Slf4j
public class TestCompletableFuture {
    public static void main(String[] args) throws InterruptedException {
        final var executor = Executors.newFixedThreadPool(1);
        CompletableFuture.supplyAsync(()-> 100)
                .handleAsync((i, ex)-> {
                    log.info("1st int: {}", i);
                    return i;
                }, executor)
                .handle((i, ex)-> {
                    log.info("2nd int: {}", i);
                    return i;
                })
                .handle((i, ex)-> {
                    log.info("3rd int: {}", i);
                    return i;
                })
        ;

        CompletableFuture.<Integer>failedFuture(new RuntimeException("testFailed"))
                .whenCompleteAsync((i, ex)-> log.info("1st => int: {} / ex: {}", i, ExceptionUtil.exception2detail(ex)), executor)
                .whenComplete((i, ex)-> log.info("2nd => int: {} / ex: {}", i, ExceptionUtil.exception2detail(ex)))
                .whenComplete((i, ex)-> log.info("3rd => int: {} / ex: {}", i, ExceptionUtil.exception2detail(ex)));
    }
}
