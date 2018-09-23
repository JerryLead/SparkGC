import sun.misc.JavaNioAccess;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by xulijie on 17-10-26.
 */
public class DirectMemoryTest {
    static List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
    public static void main(String[] args) {
        for (int i = 0; i < 20000; i++) {
            ByteBuffer.allocateDirect(1024 * 100);  //100K
        }

        System.out.println(sun.misc.SharedSecrets.getJavaNioAccess().getDirectBufferPool().getMemoryUsed() / 1024 / 1024 + " MB");
        System.out.println(sun.misc.VM.maxDirectMemory() / 1024 / 1024 + " MB");



        long start = System.currentTimeMillis();
        for (BufferPoolMXBean b : pools) {
            if (b.getName().equals("direct")) {
                System.out.println("[" + b.getObjectName() + "] ");
                System.out.println("used = " + b.getMemoryUsed() / 1024 / 1024 + " MB");
                System.out.println("total = " + b.getTotalCapacity() / 1024 / 1024 + " MB");
            }
        }
        long end = System.currentTimeMillis();

        System.out.println("[Time cost] " + (end - start) + " ms");

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("^^^^^^^^^^^");
    }
}

