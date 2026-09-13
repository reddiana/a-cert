package com.batchservice.cluster.fsm;

/**
 * Raft 로그로 복제되는 FSM 명령 유형 (5.3.2 공통 런타임 규약 2).
 */
public enum CommandType {
    NO_OP,

    LOCK_ACQUIRE,
    LOCK_RENEW,
    LOCK_RELEASE,
    LOCK_EXPIRE,
    LOCK_CANCEL,

    QUEUE_ENQUEUE,
    QUEUE_DEQUEUE,
    QUEUE_RENEW,
    QUEUE_ACK,
    QUEUE_REQUEUE,
    QUEUE_REASSIGN,

    JOURNAL_RECORD,

    MEMBER_STATUS,
    SYNC_CHECKPOINT
}
