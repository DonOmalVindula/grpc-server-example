package com.cw2server;

public interface DistributedTxListener {
    void onGlobalCommit();
    void onGlobalAbort();
}