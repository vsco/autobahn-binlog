package binlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSemiSyncIndicatorIsCorrect(t *testing.T) {
	assert.Equal(t, SemiSyncIndicator, byte(0xef))
}

type eventTypeToStringTest struct {
	input EventType
	want  string
}

func TestEventsAreMappedCorrectly(t *testing.T) {
	tests := []eventTypeToStringTest{
		{UNKNOWN_EVENT, "UnknownEvent"},
		{START_EVENT_V3, "StartEventV3"},
		{QUERY_EVENT, "QueryEvent"},
		{STOP_EVENT, "StopEvent"},
		{ROTATE_EVENT, "RotateEvent"},
		{INTVAR_EVENT, "IntVarEvent"},
		{LOAD_EVENT, "LoadEvent"},
		{SLAVE_EVENT, "SlaveEvent"},
		{CREATE_FILE_EVENT, "CreateFileEvent"},
		{APPEND_BLOCK_EVENT, "AppendBlockEvent"},
		{EXEC_LOAD_EVENT, "ExecLoadEvent"},
		{DELETE_FILE_EVENT, "DeleteFileEvent"},
		{NEW_LOAD_EVENT, "NewLoadEvent"},
		{RAND_EVENT, "RandEvent"},
		{USER_VAR_EVENT, "UserVarEvent"},
		{FORMAT_DESCRIPTION_EVENT, "FormatDescriptionEvent"},
		{XID_EVENT, "XidEvent"},
		{BEGIN_LOAD_QUERY_EVENT, "BeginLoadQueryEvent"},
		{EXECUTE_LOAD_QUERY_EVENT, "ExecuteLoadQueryEvent"},
		{TABLE_MAP_EVENT, "TableMapEvent"},
		{PRE_GA_WRITE_ROWS_EVENT, "PreGAWriteRowsEvent"},
		{PRE_GA_UPDATE_ROWS_EVENT, "PreGAUpdateRowsEvent"},
		{PRE_GA_DELETE_ROWS_EVENT, "PreGADeleteRowsEvent"},
		{WRITE_ROWS_EVENT_V1, "WriteRowsEventV1"},
		{UPDATE_ROWS_EVENT_V1, "UpdateRowsEventV1"},
		{DELETE_ROWS_EVENT_V1, "DeleteRowsEventV1"},
		{INCIDENT_EVENT, "IncidentEvent"},
		{HEARTBEAT_EVENT, "HeartbeatEvent"},
		{IGNORABLE_EVENT, "IgnorableEvent"},
		{ROWS_QUERY_EVENT, "RowsQueryEvent"},
		{WRITE_ROWS_EVENT_V2, "WriteRowsEventV2"},
		{UPDATE_ROWS_EVENT_V2, "UpdateRowsEventV2"},
		{DELETE_ROWS_EVENT_V2, "DeleteRowsEventV2"},
		{GTID_LOG_EVENT, "GTIDLogEvent"},
		{ANONYMOUS_GTID_LOG_EVENT, "AnonymousGTIDLogEvent"},
		{PREVIOUS_GTIDS_LOG_EVENT, "PreviousGTIDsLogEvent"},
		{0xff, "UnknownEvent"},
	}

	for _, ett := range tests {
		out := ett.input.String()
		assert.Equal(t, ett.want, out)
	}
}
