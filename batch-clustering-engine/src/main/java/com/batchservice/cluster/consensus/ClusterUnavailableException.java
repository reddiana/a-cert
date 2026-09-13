package com.batchservice.cluster.consensus;

/**
 * 타임아웃 내에 과반수 합의로 커밋하지 못한 경우 (Quorum 붕괴, 네트워크 분할의 소수파 등).
 */
public class ClusterUnavailableException extends RuntimeException {
    public ClusterUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
