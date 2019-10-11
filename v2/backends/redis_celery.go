package backends

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ehengao/machinery/v2/common"
	"github.com/ehengao/machinery/v2/config"
	"github.com/ehengao/machinery/v2/tasks"
	"github.com/RichardKnop/redsync"
	"github.com/gomodule/redigo/redis"
)

// RedisCeleryBackend represents a Redis result backend
type RedisCeleryBackend struct {
	Backend
	host       string
	password   string
	db         int
	pool       *redis.Pool
	socketPath string
	redsync    *redsync.Redsync
	groupsync  *sync.Map
	common.RedisConnector
}

// NewRedisBackend creates RedisCeleryBackend instance
func NewRedisCeleryBackend(cnf *config.Config, host, password, socketPath string, db int) Interface {
	return &RedisCeleryBackend{
		Backend:    New(cnf),
		host:       host,
		db:         db,
		password:   password,
		socketPath: socketPath,
		groupsync:  new(sync.Map),
	}
}

// InitGroup creates and saves a group meta data object
func (b *RedisCeleryBackend) InitGroup(groupUUID string, taskUUIDs []string) error {
	return nil
}

// GroupCompleted returns true if all tasks in a group finished
func (b *RedisCeleryBackend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	if currentTaskCount, ok := b.groupsync.Load(groupUUID); ok {
		if currentTaskCount.(int) >= groupTaskCount {
			return true, nil
		} else {
			return false, nil
		}
	}
	return false, fmt.Errorf("no key in group sync map for: %s", groupUUID)
}

// GroupTaskStates returns states of all tasks in the group
// This should only be called when all the tasks is completed
func (b *RedisCeleryBackend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	states := []*tasks.TaskState{}
	groupMeta, err := b.getGroupResults(groupUUID, groupTaskCount)
	if err != nil {
		return states, err
	}
	if len(groupMeta) != groupTaskCount {
		return states, fmt.Errorf("Group:%s is not completed", groupUUID)
	}
	for _, gm := range groupMeta {
		st := &tasks.TaskState{
			TaskUUID: gm.TaskID,
			State:    gm.State,
			Results:  gm.Result,
		}
		if gm.ErrorResult != nil {
			st.Error = gm.ErrorResult.ExcMsg[0]
		}
		states = append(states, st)
	}
	return states, nil
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *RedisCeleryBackend) TriggerChord(groupUUID string) (bool, error) {
	//Should always trigger is group is completed and callback is not nil
	return true, nil
}

// SetStatePending updates task state to PENDING
func (b *RedisCeleryBackend) SetStatePending(signature *tasks.Signature) error {
	// taskState := tasks.NewPendingTaskState(signature)
	// return b.updateState(taskState)
	return nil
}

// SetStateReceived updates task state to RECEIVED
func (b *RedisCeleryBackend) SetStateReceived(signature *tasks.Signature) error {
	// taskState := tasks.NewReceivedTaskState(signature)
	// return b.updateState(taskState)
	return nil
}

// SetStateStarted updates task state to STARTED
func (b *RedisCeleryBackend) SetStateStarted(signature *tasks.Signature) error {
	// taskState := tasks.NewStartedTaskState(signature)
	// return b.updateState(taskState)
	return nil
}

// SetStateRetry updates task state to RETRY
func (b *RedisCeleryBackend) SetStateRetry(signature *tasks.Signature) error {
	state := tasks.NewRetryTaskState(signature)
	return b.updateState(state)
}

// SetStateSuccess updates task state to SUCCESS
func (b *RedisCeleryBackend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	taskState := tasks.NewSuccessTaskState(signature, results)
	err := b.updateState(taskState)
	if err != nil {
		return err
	}
	if signature.GroupUUID != "" && signature.ChordCallback != nil {
		cgr := &tasks.CeleryGroupResult{
			TaskID: signature.UUID,
			State:  "SUCCESS",
			Result: results,
		}
		b.updateGroupResult(signature.GroupUUID, cgr)
	}
	return nil
}

// SetStateFailure updates task state to FAILURE
func (b *RedisCeleryBackend) SetStateFailure(signature *tasks.Signature, errr string) error {
	taskState := tasks.NewFailureTaskState(signature, errr)
	err := b.updateState(taskState)
	if err != nil {
		return err
	}
	results := &tasks.CeleryErrorResult{
		ExcType: "GolangError",
		ExcMsg:  []string{errr},
	}
	if signature.GroupUUID != "" && signature.ChordCallback != nil {
		cgr := &tasks.CeleryGroupResult{
			TaskID:      signature.UUID,
			State:       "FAILURE",
			ErrorResult: results,
		}
		b.updateGroupResult(signature.GroupUUID, cgr)
	}
	return nil
}

// GetState returns the latest task state
func (b *RedisCeleryBackend) GetState(taskUUID string) (*tasks.TaskState, error) {
	conn := b.open()
	defer conn.Close()

	item, err := redis.Bytes(conn.Do("GET", taskUUID))
	if err != nil {
		return nil, err
	}

	state := new(tasks.TaskState)
	decoder := json.NewDecoder(bytes.NewReader(item))
	decoder.UseNumber()
	if err := decoder.Decode(state); err != nil {
		return nil, err
	}

	return state, nil
}

// PurgeState deletes stored task state
func (b *RedisCeleryBackend) PurgeState(taskUUID string) error {
	conn := b.open()
	defer conn.Close()

	_, err := conn.Do("DEL", taskUUID)
	if err != nil {
		return err
	}

	return nil
}

// PurgeGroupMeta deletes stored group meta data
func (b *RedisCeleryBackend) PurgeGroupMeta(groupUUID string) error {
	conn := b.open()
	defer conn.Close()

	_, err := conn.Do("DEL", fmt.Sprintf("celery-task-meta-%s.j", groupUUID))
	if err != nil {
		return err
	}
	b.groupsync.Delete(groupUUID)
	return nil
}

func (b *RedisCeleryBackend) getGroupLength(groupUUID string) (length int64, err error) {
	conn := b.open()
	defer conn.Close()
	r, err := conn.Do("LLEN", fmt.Sprintf("celery-task-meta-%s.j", groupUUID))
	if err != nil {
		return 0, err
	}
	if l, ok := r.(int64); ok {
		length = l
		fmt.Printf("No Error:%s\n", length)
	} else {
		err = fmt.Errorf("length type error:%s", r)
	}
	return
}

// getGroupMeta retrieves group meta data, convenience function to avoid repetition
func (b *RedisCeleryBackend) getGroupResults(groupUUID string, count int) ([]*tasks.CeleryGroupResult, error) {
	conn := b.open()
	defer conn.Close()
	r, err := conn.Do("LRANGE", fmt.Sprintf("celery-task-meta-%s.j", groupUUID), 0, -1)
	if err != nil {
		return nil, err
	}
	var groupMeta []*tasks.CeleryGroupResult
	if rw, ok := r.([]interface{}); ok {
		for _, rwr := range rw {
			bts, err := redis.Bytes(rwr, nil)
			if err != nil {
				return nil, err
			}
			gm := new(tasks.CeleryGroupResult)
			decoder := json.NewDecoder(bytes.NewReader(bts))
			decoder.UseNumber()
			if err := decoder.Decode(gm); err != nil {
				return nil, err
			}
			groupMeta = append(groupMeta, gm)
		}
	}
	return groupMeta, nil
}

// getStates returns multiple task states
func (b *RedisCeleryBackend) getStates(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	taskStates := make([]*tasks.TaskState, len(taskUUIDs))

	conn := b.open()
	defer conn.Close()

	// conn.Do requires []interface{}... can't pass []string unfortunately
	taskUUIDInterfaces := make([]interface{}, len(taskUUIDs))
	for i, taskUUID := range taskUUIDs {
		// taskUUIDInterfaces[i] = interface{}(taskUUID)
		taskUUIDInterfaces[i] = fmt.Sprintf("celery-task-meta-%s", taskUUID)
	}
	reply, err := redis.Values(conn.Do("MGET", taskUUIDInterfaces...))
	if err != nil {
		return taskStates, err
	}

	for i, value := range reply {
		stateBytes, ok := value.([]byte)
		if !ok {
			return taskStates, fmt.Errorf("Expected byte array, instead got: %v", value)
		}

		taskState := new(tasks.CeleryTaskState)
		decoder := json.NewDecoder(bytes.NewReader(stateBytes))
		decoder.UseNumber()
		if err := decoder.Decode(taskState); err != nil {
			return taskStates, err
		}

		taskStates[i] = &tasks.TaskState{
			TaskUUID: taskState.TaskID,
			State:    taskState.Status,
			Results:  taskState.Result,
		}
	}

	return taskStates, nil
}

// updateState saves current task state
func (b *RedisCeleryBackend) updateState(taskState *tasks.TaskState) error {
	conn := b.open()
	defer conn.Close()
	celeryState := &tasks.CeleryTaskState{
		TaskID:    taskState.TaskUUID,
		Status:    taskState.State,
		Result:    taskState.Results,
		Traceback: taskState.Error,
	}
	if len(celeryState.Traceback) > 0 {
		celeryState.ErrorResult = &tasks.CeleryErrorResult{
			ExcType: "GolangError",
			ExcMsg:  []string{celeryState.Traceback},
		}
		python_traceback := fmt.Sprintf("Traceback (most recent call last):\n File \"<stdin>\", line 1, in <module>\n %s", celeryState.Traceback)
		celeryState.Traceback = python_traceback
	}
	encoded, err := json.Marshal(celeryState)
	if err != nil {
		return err
	}

	expiresIn := b.cnf.ResultsExpireIn
	if expiresIn == 0 {
		// // expire results after 1 hour by default
		expiresIn = 3600
	}
	expirationTimestamp := int64(expiresIn)
	_, err = conn.Do("MULTI")
	if err != nil {
		return err
	}
	_, err = conn.Do("SETEX", fmt.Sprintf("celery-task-meta-%s", taskState.TaskUUID), expirationTimestamp, encoded)
	if err != nil {
		return err
	}
	if taskState.State == "SUCCESS" || taskState.State == "FAILURE" {
		_, err = conn.Do("PUBLISH", fmt.Sprintf("celery-task-meta-%s", taskState.TaskUUID), encoded)
		if err != nil {
			return err
		}
	}
	_, err = conn.Do("EXEC")
	return err
}

func (b *RedisCeleryBackend) updateGroupResult(gid string, cgr *tasks.CeleryGroupResult) (length int64, err error) {
	encoded, err := json.Marshal(cgr)
	if err != nil {
		return
	}
	conn := b.open()
	defer conn.Close()
	_, err = conn.Do("MULTI")
	if err != nil {
		return
	}
	j_gid := fmt.Sprintf("celery-task-meta-%s.j", gid)
	_, err = conn.Do("RPUSH", j_gid, encoded)
	if err != nil {
		return
	}
	expiresIn := b.cnf.ResultsExpireIn
	if expiresIn == 0 {
		// // expire results after 1 hour by default
		expiresIn = 3600
	}
	expirationTimestamp := int32(time.Now().Unix() + int64(expiresIn))
	_, err = conn.Do("EXPIREAT", j_gid, expirationTimestamp)
	if err != nil {
		return
	}
	r, err := conn.Do("EXEC")
	if err != nil {
		return
	}
	if currentTaskCount, ok := b.groupsync.Load(gid); ok {
		b.groupsync.Store(gid, currentTaskCount.(int)+1)
	} else {
		b.groupsync.Store(gid, 1)
	}
	if rw, ok := r.([]interface{}); ok {
		if l, ok := rw[0].(int64); ok {
			length = l

		} else {
			err = fmt.Errorf("first element is not length type", rw[0])
		}
	} else {
		err = fmt.Errorf("cannot get length of the group", r)
	}
	return
}

// setExpirationTime sets expiration timestamp on a stored task state
func (b *RedisCeleryBackend) setExpirationTime(key string) error {
	expiresIn := b.cnf.ResultsExpireIn
	if expiresIn == 0 {
		// // expire results after 1 hour by default
		expiresIn = 3600
	}
	expirationTimestamp := int32(time.Now().Unix() + int64(expiresIn))

	conn := b.open()
	defer conn.Close()

	_, err := conn.Do("EXPIREAT", key, expirationTimestamp)
	if err != nil {
		return err
	}

	return nil
}

// open returns or creates instance of Redis connection
func (b *RedisCeleryBackend) open() redis.Conn {
	if b.pool == nil {
		b.pool = b.NewPool(b.socketPath, b.host, b.password, b.db, b.cnf.Redis)
	}
	if b.redsync == nil {
		var pools = []redsync.Pool{b.pool}
		b.redsync = redsync.New(pools)
	}
	conn := b.pool.Get()
	// if b.db > 0 {
	// 	_, e := conn.Do("SELECT", b.db)
	// 	if e != nil {
	// 		return nil, e
	// 	}
	// }
	return conn
}
