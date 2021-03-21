package com.example.source;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

public class CustomSource implements SourceFunction<String> {
    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        System.out.println("------输入流开始启动,时间：" + dateFormat.format(System.currentTimeMillis()) + "------");
        System.out.println("------在没有第一个Event进入前，不会开启窗口------");
        // 控制大约在 10 秒的倍数的时间点开始
        String currTime = String.valueOf(System.currentTimeMillis());
        while (Integer.valueOf(currTime.substring(currTime.length() - 4)) > 100) {
            currTime = String.valueOf(System.currentTimeMillis());
            continue;
        }
        TimeUnit.SECONDS.sleep(20);

        TimeUnit.SECONDS.sleep(3);
        String e1 = "a," + System.currentTimeMillis() + "," + dateFormat.format(System.currentTimeMillis());
        String e2 = "a," + System.currentTimeMillis() + "," + dateFormat.format(System.currentTimeMillis());
        String e2_2 = "a," + System.currentTimeMillis() + "," + dateFormat.format(System.currentTimeMillis());
        ctx.collect(e1);//第一个发送了,第二个事件由于网络原因，事件没有发送

        TimeUnit.SECONDS.sleep(3);
        String e3 = "a," + System.currentTimeMillis() + "," + dateFormat.format(System.currentTimeMillis());
        ctx.collect(e3);
        ctx.collect(e2);

        TimeUnit.SECONDS.sleep(3);
        String e4 = "a," + System.currentTimeMillis() + "," + dateFormat.format(System.currentTimeMillis());
        ctx.collect(e4);

        TimeUnit.SECONDS.sleep(3);
        String e5 = "a," + System.currentTimeMillis() + "," + dateFormat.format(System.currentTimeMillis());
        ctx.collect(e5);
        ctx.collect(e2_2);

        TimeUnit.SECONDS.sleep(10);//让数据流不要这么快停掉，停掉了之后，处理程序会停掉
        System.out.println("------输入流结束,结束时间：" + dateFormat.format(System.currentTimeMillis())+"------");
        System.out.println("------输入流结束后，所有等不到WaterMark的窗口都会立即关闭并计算，不再等待------");
    }

    @Override
    public void cancel() {

    }
}
