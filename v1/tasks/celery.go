package tasks

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/satori/go.uuid"
	"regexp"
	"strings"
	"time"
)

type Kwargs map[string]interface{}

type CeleryCallback struct {
	Task        string         `json:"task"`
	Args        []Arg          `json:"args"`
	Kwargs      Kwargs         `json:"kwargs"`
	Options     *CeleryOptions `json:"options"`
	SubtaskType interface{}    `json:"subtask_type"`
	ChordSize   int            `json:"chord_size"`
	Immutable   bool           `json:"immutable"`
}

func (s *Signature) UnmarshalCeleryCallback(c *CeleryCallback) (err error) {
	s.UUID = c.Options.TaskID
	s.ReplyTo = c.Options.ReplyTo
	s.Name = c.Task
	s.GroupTaskCount = c.ChordSize
	s.Args = c.Args
	s.Immutable = c.Immutable
	return nil
}

func (c *CeleryCallback) UnmarshalSignature(s *Signature) (err error) {
	if c.Options == nil {
		c.Options = new(CeleryOptions)
	}
	c.Options.TaskID = s.UUID
	c.Options.ReplyTo = s.ReplyTo
	c.Task = s.Name
	c.ChordSize = s.GroupTaskCount
	c.Args = s.Args
	c.Immutable = s.Immutable
	return nil
}

func CreateCeleryCallbacks(ss []*Signature) (cc []*CeleryCallback, err error) {
	for _, s := range ss {
		c := new(CeleryCallback)
		err = c.UnmarshalSignature(s)
		if err != nil {
			return nil, err
		}
		cc = append(cc, c)
	}
	return cc, nil
}

func CreateSignatureFromCallbacks(cc []*CeleryCallback) (ss []*Signature, err error) {
	for _, c := range cc {
		s := new(Signature)
		err = s.UnmarshalCeleryCallback(c)
		if err != nil {
			return nil, err
		}
		ss = append(ss, s)
	}
	return ss, nil
}

type CeleryOptions struct {
	TaskID  string `json:"task_id"`
	ReplyTo string `json:"reply_to"`
}

type Embed struct {
	Callbacks []*CeleryCallback `json:"callbacks"`
	Errbacks  []*CeleryCallback `json:"errbacks"`
	Chain     *CeleryCallback   `json:"chain"`
	Chord     *CeleryCallback   `json:"chord"`
}

func (e *Embed) UnmarshalSignature(s *Signature) (err error) {
	var callbacks []*CeleryCallback
	var errbacks []*CeleryCallback
	var chord *CeleryCallback
	if s.OnSuccess != nil {
		callbacks, err = CreateCeleryCallbacks(s.OnSuccess)
	}
	if s.OnError != nil {
		callbacks, err = CreateCeleryCallbacks(s.OnError)
	}
	if s.ChordCallback != nil {
		chord := new(CeleryCallback)
		err = chord.UnmarshalSignature(s.ChordCallback)
	}
	e.Callbacks = callbacks
	e.Errbacks = errbacks
	e.Chord = chord
	return err
}

type Timelimit []int

type CeleryMessage struct {
	Properties CeleryMessageProperties
	Headers    CeleryMessageHeaders
	Body       *CeleryMessageBody
}

type CeleryMessageProperties struct {
	CorrelationId   uuid.UUID `json:"correlation_id"`
	ContentType     string    `json:"content_type"`
	ContentEncoding string    `json:"content_encoding"`
	//optional
	ReplyTo string `json:"reply_to"`
}
type CeleryMessageHeaders struct {
	Lang     string `mapstructure:"lang"`
	Task     string `mapstructure:"task"`
	Id       string `mapstructure:"id"`
	RootId   string `mapstructure:"root_id"`
	ParentId string `mapstructure:"parent_id"`
	Group    string `mapstructure:"group"`

	// optional
	Meth       string     `mapstructure:"meth"`
	Shadow     string     `mapstructure:"shadow"`
	ETA        *time.Time `mapstructure:"eta"`
	Expires    *time.Time `mapstructure:"expires"`
	Retries    int        `mapstructure:"retries"`
	Timelimit  Timelimit  `mapstructure:"timelimit"`
	Argsrepr   string     `mapstructure:"argsrepr"`
	Kwargsrepr string     `mapstructure:"kwargsrepr"`
	Origin     string     `mapstructure:"origin"`
	BatchSize  int        `mapstructure:"batch_size"`
}

func (s *Signature) UnmarshalCeleryHeader(header *CeleryMessageHeaders) (err error) {
	s.Name = header.Task
	s.UUID = header.Id
	s.ETA = header.ETA
	s.GroupUUID = header.Group
	s.RetryCount = header.Retries
	s.RetryTimeout = header.Timelimit[1]
	return nil
}

type CeleryMessageBody struct {
	Args   []Arg
	Kwargs Kwargs
	Embed  Embed
}

type CeleryMessageBodyBatch struct {
	Args   [][]Arg
	Kwargs []Kwargs
	Embed  Embed
}

func (cmb *CeleryMessageBody) UnmarshalJSON(b []byte) (err error) {
	d := []interface{}{}
	args := []Arg{}
	kwargs := new(Kwargs)
	embed := new(Embed)
	d = append(d, &args)
	d = append(d, kwargs)
	d = append(d, embed)
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	if err = decoder.Decode(&d); err != nil {
		return err
	}
	cmb.Args = args
	cmb.Kwargs = *kwargs
	cmb.Embed = *embed
	return nil
}

func (cmb *CeleryMessageBodyBatch) UnmarshalJSON(b []byte) (err error) {
	d := []interface{}{}
	args := [][]Arg{}
	kwargs := []Kwargs{}
	ai := []interface{}{}
	embed := new(Embed)
	var k map[string]interface{}
	ai = append(ai, &args)
	ai = append(ai, &kwargs)
	d = append(d, &ai)
	d = append(d, &k)
	d = append(d, embed)
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	if err = decoder.Decode(&d); err != nil {
		return err
	}
	cmb.Args = args
	cmb.Kwargs = kwargs
	cmb.Embed = *embed
	return nil
}

type CeleryErrorResult struct {
	ExcType string `json:"exc_type"`
	ExcMsg  string `json:"exc_message"`
}

type CeleryTaskState struct {
	TaskID      string             `json:"task_id"`
	Status      string             `json:"status"`
	Result      []*TaskResult      `json:"result"`
	ErrorResult *CeleryErrorResult `json:"error_result,omitempty"`
	Traceback   string             `json:"traceback"`
	Children    []*CeleryTaskState `json:"children"`
}

type celeryTaskStateOk struct {
	TaskID   string             `json:"task_id"`
	Status   string             `json:"status"`
	Result   []*TaskResult      `json:"result"`
	Children []*CeleryTaskState `json:"children"`
}

type celeryTaskStateNok struct {
	TaskID      string             `json:"task_id"`
	Status      string             `json:"status"`
	ErrorResult *CeleryErrorResult `json:"result"`
	Traceback   string             `json:"traceback"`
	Children    []*CeleryTaskState `json:"children"`
}

func (c *CeleryTaskState) MarshalJSON() ([]byte, error) {
	if c.ErrorResult != nil {
		manok := &celeryTaskStateNok{
			TaskID:      c.TaskID,
			Status:      c.Status,
			ErrorResult: c.ErrorResult,
			Traceback:   c.Traceback,
			Children:    c.Children,
		}
		return json.Marshal(manok)
	}
	maok := &celeryTaskStateOk{
		TaskID:   c.TaskID,
		Status:   c.Status,
		Result:   c.Result,
		Children: c.Children,
	}

	return json.Marshal(maok)

}

func (c *CeleryTaskState) UnmarshalJSON(b []byte) error {
	if strings.Contains(string(b), "\"exc_type\":") {
		manok := new(celeryTaskStateNok)
		err := json.Unmarshal(b, manok)
		if err != nil {
			return nil
		}
		c.TaskID = manok.TaskID
		c.Status = manok.Status
		c.ErrorResult = manok.ErrorResult
		c.Traceback = manok.Traceback
		c.Children = manok.Children
	}
	maok := new(celeryTaskStateOk)
	err := json.Unmarshal(b, maok)
	if err != nil {
		return nil
	}
	c.TaskID = maok.TaskID
	c.Status = maok.Status
	c.Result = maok.Result
	c.Children = maok.Children
	return nil
}

// IsCompleted returns true if state is SUCCESS or FAILURE,
// i.e. the task has finished processing and either succeeded or failed.
func (taskState *CeleryTaskState) IsCompleted() bool {
	return taskState.IsSuccess() || taskState.IsFailure()
}

// IsSuccess returns true if state is SUCCESS
func (taskState *CeleryTaskState) IsSuccess() bool {
	return taskState.Status == "SUCCESS"
}

// IsFailure returns true if state is FAILURE
func (taskState *CeleryTaskState) IsFailure() bool {
	return taskState.Status == "FAILURE"
}

type CeleryGroupResult struct {
	TaskID      string             `json:"task_id"`
	State       string             `json:"state"`
	Result      []*TaskResult      `json:"result"`
	ErrorResult *CeleryErrorResult `json:"error_result,omitempty"`
}

func (c *CeleryGroupResult) MarshalJSON() ([]byte, error) {
	if c.ErrorResult == nil {
		r := []interface{}{
			1,
			c.TaskID,
			c.State,
			c.Result,
		}
		return json.Marshal(r)
	} else {
		er := []interface{}{
			1,
			c.TaskID,
			c.State,
			c.ErrorResult,
		}
		return json.Marshal(er)
	}
}

func (c *CeleryGroupResult) UnmarshalJSON(b []byte) error {
	r, err := regexp.Compile(`\[1,\s*\"(\S+)\",\s*\"(\S+)\",\s*(\[\{\S.*\S\}\])\]$`)
	if err != nil {
		return err
	}
	er, err := regexp.Compile(`\[1,\s*\"(\S+)\",\s*\"(\S+)\",\s*(\[\{\S.*\S\}\])\]$`)
	if err != nil {
		return err
	}
	if matched := r.FindStringSubmatch(string(b)); len(matched) == 4 {
		c.TaskID = matched[1]
		c.State = matched[2]
		decoder := json.NewDecoder(bytes.NewReader([]byte(matched[3])))
		decoder.UseNumber()
		var result []*TaskResult
		if err = decoder.Decode(&result); err != nil {
			return err
		}
		c.Result = result
	} else if ematched := er.FindStringSubmatch(string(b)); len(ematched) == 4 {
		c.TaskID = ematched[1]
		c.State = ematched[2]
		decoder := json.NewDecoder(bytes.NewReader([]byte(ematched[3])))
		decoder.UseNumber()
		err_result := new(CeleryErrorResult)
		if err = decoder.Decode(&err_result); err != nil {
			return err
		}
		c.ErrorResult = err_result
	} else {
		return fmt.Errorf("group result format is not celery standard: %s", b)
	}
	return nil
}
