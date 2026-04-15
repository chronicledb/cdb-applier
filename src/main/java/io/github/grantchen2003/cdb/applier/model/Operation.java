package io.github.grantchen2003.cdb.applier.model;

public record Operation(
        OpType opType,
        String table,
        String data
) {
    public enum OpType { SET, DELETE }
}