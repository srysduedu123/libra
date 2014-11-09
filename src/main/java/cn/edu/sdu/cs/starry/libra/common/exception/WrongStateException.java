package cn.edu.sdu.cs.starry.libra.common.exception;

/**
 * Created with IntelliJ IDEA.
 * User: xccui
 * Date: 13-10-14
 * Time: 上午10:18
 * To change this template use File | Settings | File Templates.
 */
public class WrongStateException extends ExecutorException {

    public WrongStateException(String message) {
        super(message);
    }

    public WrongStateException(Throwable throwable) {
        super(throwable);
    }
}
