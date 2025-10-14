package config

import (
	"math"
	"os"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Helper functions to read environment variables.
func getEnvInt(key string, def int) int {
	val := os.Getenv(key)
	if val == "" {
		return def
	}

	i, err := strconv.Atoi(val)
	if err != nil {
		return def
	}

	return i
}

func getEnvInt64(key string, def int64) int64 {
	val := os.Getenv(key)
	if val == "" {
		return def
	}

	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return def
	}

	return i
}

func getEnvByteSize(key string, def int) int {
	val := os.Getenv(key)
	if val == "" {
		return def
	}

	i, err := humanize.ParseBytes(val)
	if err != nil {
		return def
	}

	return int(i)
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	val := os.Getenv(key)
	if val == "" {
		return def
	}

	d, err := time.ParseDuration(val)
	if err != nil {
		return def
	}

	return d
}

// MongoLogEnabled returns true if MongoDB logging is enabled via the MONGO_LOG_ENABLED environment variable.
func MongoLogEnabled() bool {
	return os.Getenv("MONGO_LOG_ENABLED") == "true"
}

func MongoLogComponent() options.LogComponent {
	return options.LogComponentAll // Not configurable via env
}

func MongoLogLevel() options.LogLevel {
	return options.LogLevelInfo // Not configurable via env
}

// PLMDatabase returns the MongoDB database name, configurable via the PLM_DATABASE environment variable.
func PLMDatabase() string {
	return getEnvStr("PLM_DATABASE", "percona_link_mongodb")
}

func RecoveryCollection() string {
	return getEnvStr("RECOVERY_COLLECTION", "checkpoints")
}

func HeartbeatCollection() string {
	return getEnvStr("HEARTBEAT_COLLECTION", "heartbeats")
}

func getEnvStr(key, def string) string {
	val := os.Getenv(key)
	if val == "" {
		return def
	}

	return val
}

func RecoveryCheckpointingInternal() time.Duration {
	return getEnvDuration("RECOVERY_CHECKPOINTING_INTERVAL", 15*time.Second)
}

func HeartbeatInternal() time.Duration {
	return getEnvDuration("HEARTBEAT_INTERVAL", 30*time.Second)
}

func HeartbeatTimeout() time.Duration {
	return getEnvDuration("HEARTBEAT_TIMEOUT", 30*time.Second)
}

func StaleHeartbeatDuration() time.Duration {
	return HeartbeatInternal() + HeartbeatTimeout()
}

func PingTimeout() time.Duration {
	return getEnvDuration("PING_TIMEOUT", 60*time.Second)
}

func DisconnectTimeout() time.Duration {
	return getEnvDuration("DISCONNECT_TIMEOUT", 5*time.Second)
}

func CloseCursorTimeout() time.Duration {
	return getEnvDuration("CLOSE_CURSOR_TIMEOUT", 10*time.Second)
}

func DefaultMongoDBCliOperationTimeout() time.Duration {
	return getEnvDuration("MONGODB_CLI_OPERATION_TIMEOUT", 30*time.Minute)
}

// ChangeStreamBatchSize returns the batch size for change streams, configurable via the CHANGE_STREAM_BATCH_SIZE environment variable.
func ChangeStreamBatchSize() int {
	return getEnvInt("CHANGE_STREAM_BATCH_SIZE", 1000)
}

func ChangeStreamAwaitTime() time.Duration {
	return getEnvDuration("CHANGE_STREAM_AWAIT_TIME", time.Second)
}

func ReplQueueSize() int {
	return getEnvInt("REPL_QUEUE_SIZE", ChangeStreamBatchSize())
}

func BulkOpsSize() int {
	return getEnvInt("BULK_OPS_SIZE", ChangeStreamBatchSize())
}

func BulkOpsInterval() time.Duration {
	return getEnvDuration("BULK_OPS_INTERVAL", ChangeStreamAwaitTime())
}

func InitialSyncCheckInterval() time.Duration {
	return getEnvDuration("INITIAL_SYNC_CHECK_INTERVAL", 10*time.Second)
}

func PrintLagTimeInterval() time.Duration {
	return getEnvDuration("PRINT_LAG_TIME_INTERVAL", InitialSyncCheckInterval())
}

// Wire protocol and batch sizes.
func WireProtoMsgReserveSizeBytes() int {
	return getEnvInt("WIRE_PROTO_MSG_RESERVE_SIZE_BYTES", 512)
}

func MaxBSONSize() int {
	return getEnvInt("MAX_BSON_SIZE", 16*humanize.MiByte)
}

func MaxMessageSizeBytes() int {
	return getEnvInt("MAX_MESSAGE_SIZE_BYTES", 48*humanize.MByte)
}

func MaxWriteBatchSizeBytes() int {
	return MaxMessageSizeBytes() - WireProtoMsgReserveSizeBytes()
}

func MaxWriteBatchSize() int {
	return getEnvInt("MAX_WRITE_BATCH_SIZE", 100_000)
}

// Clone and segment settings getter functions.
func DefaultCloneNumParallelCollection() int {
	return getEnvInt("DEFAULT_CLONE_NUM_PARALLEL_COLLECTION", 2)
}

func AutoCloneSegmentSize() int {
	return getEnvInt("AUTO_CLONE_SEGMENT_SIZE", 0)
}

func MinCloneSegmentSizeBytes() int {
	return getEnvInt("MIN_CLONE_SEGMENT_SIZE_BYTES", 10*MaxWriteBatchSizeBytes())
}

func MaxCloneSegmentSizeBytes() int {
	return getEnvInt("MAX_CLONE_SEGMENT_SIZE_BYTES", 64*humanize.GiByte)
}

func DefaultCloneReadBatchSizeBytes() int {
	return getEnvInt("DEFAULT_CLONE_READ_BATCH_SIZE_BYTES", MaxWriteBatchSizeBytes())
}

func MinCloneReadBatchSizeBytes() int {
	return getEnvInt("MIN_CLONE_READ_BATCH_SIZE_BYTES", MaxBSONSize())
}

func MaxCloneReadBatchSizeBytes() int {
	return getEnvInt("MAX_CLONE_READ_BATCH_SIZE_BYTES", math.MaxInt32)
}

func MaxInsertBatchSize() int {
	return getEnvInt("MAX_INSERT_BATCH_SIZE", 10_000)
}

func MaxInsertBatchSizeBytes() int {
	return getEnvInt("MAX_INSERT_BATCH_SIZE_BYTES", MaxBSONSize())
}
