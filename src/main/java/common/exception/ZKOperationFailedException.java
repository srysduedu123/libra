package common.exception;

/**
 * @author xccui
 * Date: 14-10-20
 * Time: 9:39
 */
public class ZKOperationFailedException extends Exception {

    public ZKOperationFailedException(String message) {
        super(message);
    }

    public ZKOperationFailedException(Throwable throwable) {
        super(throwable);
    }
}
