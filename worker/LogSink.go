package worker

import (
	"context"
	"github.com/xyz2b/crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// mongodb存储日志
type LogSink struct {
	client *mongo.Client
	logCollection *mongo.Collection
	logChan chan * common.JobLog
	autoCommintChan chan *common.LogBatch
}

var (
	G_logSink *LogSink
)

func InitLogSink() (err error){
	var (
		client *mongo.Client
	)

	// 建立mongodb连接
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(G_config.MongodbConnectTimeout) * time.Millisecond)
	if client,err = mongo.Connect(ctx,options.Client().ApplyURI(G_config.MongodbUri)); err != nil {
		return err
	}

	// 选择db和collection
	G_logSink = &LogSink{
		client:client,
		logCollection:client.Database("cron").Collection("log"),
		logChan:make(chan * common.JobLog, 1000),
		autoCommintChan: make(chan *common.LogBatch, 1000),
	}

	// 启动一个mongodb处理协程
	go G_logSink.writeLoop()

	return err
}

//日志存储协程
func (logSink *LogSink) writeLoop()  {
	var (
		log *common.JobLog
		logBatch *common.LogBatch // 当前批次
		commitTimer *time.Timer
		timeoutBatch *common.LogBatch // 超时批次

	)

	for {
		select {
		case log = <- logSink.logChan:
			// 把这条log写到mongodb中
			// 每次插入需要等待mongodb的一次请求往返，耗时可能因为网络慢花费比较长的时间

			if logBatch == nil{
				logBatch = &common.LogBatch{}
				// 让这个批次超时自动提交（给1秒的时间）
				commitTimer = time.AfterFunc(time.Duration(G_config.JobLogCommitTimeout)*time.Millisecond, func(logBatch *common.LogBatch) func() {
					// 发出超时通知，不要直接提交batch，因为AfterFunc的回调函数是在另一个协程中，那样的话就是两个协程操作batch，并发操作，不安全
					// 定义一个函数立即调用，并返回一个函数(AfterFunc的回调函数)，避免闭包的影响，将变量直接复制进来
					return func() {
						logSink.autoCommintChan <- logBatch
					}
				}(logBatch))
			}

			// 把新的日志追加到批次中
			logBatch.Logs = append(logBatch.Logs, log)

			// 如果批次满了，就立即发送
			if len(logBatch.Logs) >= G_config.JobLogBatchSize {
				// 发送日志
				logSink.saveLogs(logBatch)
				// 清空logBatch
				logBatch = nil
				// 取消定时器
				commitTimer.Stop()
			}

		case timeoutBatch = <- logSink.autoCommintChan: // 过期的批次
			// 判断过期批次是否仍旧是当前批次
			if timeoutBatch != logBatch{
				continue // 跳过已经被提交的批次
			}

			// 把批次写入到mongodb中
			logSink.saveLogs(timeoutBatch)
			// 清空logBatch
			logBatch = nil
		}
	}
}

// 批量写入日志
func (logSink *LogSink) saveLogs(batch *common.LogBatch)  {
	logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
}

// 发送日志
func (logSink *LogSink) Append(joblog *common.JobLog)  {
	select {
	case logSink.logChan <- joblog:
	default:
		// 队列满了就丢弃
	}

}