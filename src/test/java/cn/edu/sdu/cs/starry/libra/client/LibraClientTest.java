package cn.edu.sdu.cs.starry.libra.client;

import cn.edu.sdu.cs.starry.libra.common.exception.ConfigException;
import org.apache.zookeeper.KeeperException;
import cn.edu.sdu.cs.starry.libra.test.AnotherTestExecutor;
import cn.edu.sdu.cs.starry.libra.test.TestExecutor;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * @author xccui
 * Date: 14-10-19
 * Time: 下午3:59
 */
public class LibraClientTest {
    public static void main(String args[]) {
        try {
            ExecutorFactory factory = new ExecutorFactory();
            factory.addExecutor("project1", new TestExecutor());
            factory.addExecutor("project2", new AnotherTestExecutor());
            LibraClient watcher = new LibraClient("id1", "ubuntuB61:2181,ubuntuB70:2181", 20000, factory);
            watcher.start();
            Thread.sleep(Integer.MAX_VALUE);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ConfigException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
