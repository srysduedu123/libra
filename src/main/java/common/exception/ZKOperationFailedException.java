package common.exception;

/**
 * Created with IntelliJ IDEA.
 * User: xccui
 * Date: 13-10-13
 * Time: 下午8:13
 * To change this template use File | Settings | File Templates.
 */
public class ZKOperationFailedException extends Exception {
    public ZKOperationFailedException(String message) {
        super(message);
    }

    public ZKOperationFailedException(Throwable throwable) {
        super(throwable);
    }
}
