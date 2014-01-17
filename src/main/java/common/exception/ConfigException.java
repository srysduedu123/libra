package common.exception;

/**
 * Created with IntelliJ IDEA.
 * User: xccui
 * Date: 13-10-9
 * Time: 下午5:57
 * To change this template use File | Settings | File Templates.
 */
public class ConfigException extends Exception {
    public ConfigException(String message) {
        super(message);
    }

    public ConfigException(Throwable throwable) {
        super(throwable);
    }
}
