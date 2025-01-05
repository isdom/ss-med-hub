package com.yulore.util;

import com.google.common.base.Charsets;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;

public class ExceptionUtil {

    public static String exception2detail(Throwable throwable) {
        if (throwable instanceof InvocationTargetException) {
            throwable = ((InvocationTargetException)throwable).getTargetException();
        }

        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final PrintWriter writer = new PrintWriter(os);
        throwable.printStackTrace(writer);
        writer.flush();

        return os.toString(Charsets.UTF_8);
    }

    public static String dumpCallStack(final Throwable throwable, final String prefix, int skipDepth) {
        final StringBuilder sb = new StringBuilder();
        final StackTraceElement[] callStacks = throwable.getStackTrace();

        if ( null != prefix ) {
            sb.append(prefix);
        }

        for ( StackTraceElement cs : callStacks) {
            if ( skipDepth > 0 ) {
                skipDepth--;
                continue;
            }
            sb.append('\r');
            sb.append('\n');
            sb.append("\tat ");
            sb.append(cs);
        }

        return sb.toString();
    }
}