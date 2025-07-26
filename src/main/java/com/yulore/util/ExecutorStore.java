package com.yulore.util;

import java.util.concurrent.Executor;
import java.util.function.Function;

public interface ExecutorStore extends Function<String, Executor> {
}
