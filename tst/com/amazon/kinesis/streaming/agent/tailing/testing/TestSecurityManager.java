package com.amazon.kinesis.streaming.agent.tailing.testing;

import java.security.Permission;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Security manager for testing that captures System.exit calls.
 * Used to verify shutdown behavior in tests without actually terminating the JVM.
 */
public class TestSecurityManager extends SecurityManager {
    private final AtomicBoolean exitCalled;

    public TestSecurityManager(AtomicBoolean exitCalled) {
        this.exitCalled = exitCalled;
    }

    @Override
    public void checkPermission(Permission perm) {
        // Allow all other permissions
    }

    @Override
    public void checkExit(int status) {
        exitCalled.set(true);
        throw new SecurityException("System.exit(" + status + ") intercepted");
    }
}
