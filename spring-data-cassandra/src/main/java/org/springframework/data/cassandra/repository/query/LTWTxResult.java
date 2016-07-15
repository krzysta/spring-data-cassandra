package org.springframework.data.cassandra.repository.query;

public class LTWTxResult<T> {

    boolean wasApplied;
    
    T offending;

    public static <T> LTWTxResult<T> ok() {
        LTWTxResult<T> result = new LTWTxResult<T>();
        result.wasApplied = true;
        return result;
    }
    
    public static <T> LTWTxResult<T> offending(T offending) {
        LTWTxResult<T> result = new LTWTxResult<T>();
        result.offending = offending;
        return result;
    }
    
    public boolean wasApplied() {
        return wasApplied;
    }

    public T getOffending() {
        return offending;
    }

    @Override
    public String toString() {
        return "LTWTxResult [wasApplied=" + wasApplied + ", offending=" + offending + "]";
    }
    
}
