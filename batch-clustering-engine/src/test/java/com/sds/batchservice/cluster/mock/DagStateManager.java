package com.sds.batchservice.cluster.mock;

import java.util.concurrent.ConcurrentHashMap;

/**
 * TC-QAS-01 테스트에서 분산 락(DistributedLockExecutor)에 의한
 * DAG 태스크 상태 전이(READY -> RUNNING -> COMPLETED) 동시성 상호 배제를 검증하기 위한 테스트용 모의 객체.
 */
public class DagStateManager {

    public enum State {
        READY,
        RUNNING,
        COMPLETED
    }

    private final ConcurrentHashMap<String, State> taskStates = new ConcurrentHashMap<>();

    public DagStateManager() {
        taskStates.put("task-A", State.COMPLETED); // Task A is finished
        taskStates.put("task-B", State.READY);     // Task B is ready to transition
    }

    public synchronized boolean transitionToRunning(String taskId) {
        State current = taskStates.get(taskId);
        if (current == State.READY) {
            taskStates.put(taskId, State.RUNNING);
            return true;
        }
        return false;
    }

    public synchronized void completeTask(String taskId) {
        taskStates.put(taskId, State.COMPLETED);
    }

    public State getState(String taskId) {
        return taskStates.get(taskId);
    }

    public void reset() {
        taskStates.clear();
        taskStates.put("task-A", State.COMPLETED);
        taskStates.put("task-B", State.READY);
    }
}
