package com.viralgains.exception

/**
 * Created by parampreet on 30/5/15.
 */
class QueryTypeNotSupportedException extends RuntimeException {
    public QueryTypeNotSupportedException(String message) {
        super(message)
    }
}
