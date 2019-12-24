package master

import (
	"context"
	"github.com/xyz2b/crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// mongodb日志管理
type LogMgr struct {
	client *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_logMgr *LogMgr
)

func InitLogMgr() (err error) {
	var (
		client *mongo.Client
	)

	// 建立mongodb连接
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(G_config.MongodbConnectTimeout)*time.Millisecond)
	if client, err = mongo.Connect(ctx, options.Client().ApplyURI(G_config.MongodbUri)); err != nil {
		return err
	}

	// 选择db和collection
	G_logMgr = &LogMgr{
		client:          client,
		logCollection:   client.Database("cron").Collection("log"),
	}

	return err
}

// 查看任务日志
func (logMgr *LogMgr) ListLog(name string, skip int64, limit int64) (logArr []*common.JobLog, err error){
	var (
		filter *common.JobLogFilter
		logSort *common.SortLogByStartTime
		findOpt *options.FindOptions
		cursor *mongo.Cursor
		jobLog *common.JobLog
	)

	// len(logArr)
	logArr = make([]*common.JobLog, 0)

	// 过滤条件
	filter = &common.JobLogFilter{JobName:name}

	// 按照任务开始时间倒排
	logSort = &common.SortLogByStartTime{SortOrder:-1}

	// 查询条件
	findOpt = options.Find()
	findOpt.Skip = &skip
	findOpt.Limit = &limit
	findOpt.Sort = logSort

	// 查询
	if cursor,err = logMgr.logCollection.Find(context.TODO(), filter, findOpt); err != nil{
		return nil, err
	}

	// 延迟释放游标
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()){
		jobLog = &common.JobLog{}

		// 反序列化bson
		if err = cursor.Decode(jobLog); err != nil{
			continue	// 有日志不合法
		}

		logArr = append(logArr, jobLog)
	}

	return logArr,nil
}
