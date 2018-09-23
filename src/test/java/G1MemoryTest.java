import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import sun.misc.Unsafe;

import javax.management.ObjectName;

/**
 * Created by xulijie on 17-10-21.
 */
public class G1MemoryTest {
    public static void main(String[] args) throws IllegalArgumentException,
            IllegalAccessException, InterruptedException {
        Field unsafeField = Unsafe.class.getDeclaredFields()[0];
        unsafeField.setAccessible(true);
        Unsafe unsafe = (Unsafe)unsafeField.get(null);
        Set<ByteBuffer> set = new HashSet<>();
        while(true) {
            long address = unsafe.allocateMemory(1024 * 1024 * 100);
            unsafe.setMemory(address, 1024 * 1024 * 100, (byte)0);
            System.out.println(sun.misc.SharedSecrets.getJavaNioAccess().getDirectBufferPool().getMemoryUsed());
            System.out.println(sun.misc.VM.maxDirectMemory() / 1024 / 1024 + " MB");
//          ByteBuffer bb = ByteBuffer.allocateDirect(1024*1024);
//          set.add(bb);
//          for (int i = 0; i < 1024*1024; i++){
//              bb.put((byte)0);
//          }
            List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);


            // print headers
            for (BufferPoolMXBean pool: pools)
                System.out.format("         %8s             ", pool.getName());
            System.out.println();
            for (int i=0; i<pools.size(); i++)
                System.out.format("%6s %10s %10s  ",  "Count", "Capacity", "Memory");
            System.out.println();

            // poll and print usage

                for (BufferPoolMXBean pool: pools) {
                    System.out.format("%6d %10d %10d  ",
                            pool.getCount(), pool.getTotalCapacity(), pool.getMemoryUsed());
                }
                System.out.println();


            Thread.sleep(5000);
            System.out.println("^^^^^^^^^^^");
        }
    }
}
