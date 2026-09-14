package com.sds.batchservice.cluster.api;

/**
 * 분산 큐에서 인출한 작업과 lease 토큰 (= 커밋된 QUEUE_DEQUEUE 엔트리 인덱스).
 * RENEW / ACK / 저널 보고 시 함께 전달하여 펜싱에 사용합니다.
 */
public record QueueLease(String jobId, long leaseToken) {
}
