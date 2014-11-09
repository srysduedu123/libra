package cn.edu.sdu.cs.starry.libra.common.exception;

/**
 * Created with IntelliJ IDEA.
 * User: xccui
 * Date: 13-10-14
 * Time: 上午10:17
 * To change this template use File | Settings | File Templates.
 */
public class ExecutorException extends Exception {
    public ExecutorException(String message) {
        super(message);
    }

    public ExecutorException(Throwable throwable) {
        super(throwable);
    }
}
