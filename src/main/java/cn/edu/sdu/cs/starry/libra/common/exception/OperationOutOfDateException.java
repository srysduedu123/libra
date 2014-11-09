package cn.edu.sdu.cs.starry.libra.common.exception;

/**
 * Created with IntelliJ IDEA.
 * User: xccui
 * Date: 13-10-14
 * Time: 上午10:30
 * To change this template use File | Settings | File Templates.
 */
public class OperationOutOfDateException extends Exception {
    public OperationOutOfDateException(String message) {
        super(message);
    }

    public OperationOutOfDateException(Throwable throwable) {
        super(throwable);
    }
}
