package com.example.flinkpipelinedemo.modules.wager.exception;

public class WagerProcessException extends RuntimeException {

    public WagerProcessException(String message) {
        super(message);
    }

    public WagerProcessException(String message, Throwable cause) {
        super(message, cause);
    }
}