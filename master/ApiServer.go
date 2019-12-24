package master

import (
	"encoding/json"
	"github.com/xyz2b/crontab/common"
	"net"
	"net/http"
	"strconv"
	"time"
)

// 任务的HTTP接口
type ApiServer struct {
	httpServer *http.Server
}

var (
	// 单例对象
	// 通过定义全局变量实现单例
	// 其他模块可以通过该单例变量访问其内部的方法
	G_apiServer *ApiServer
)

// 保存任务接口
// POST job={"name":"job1","command":"echo hello","cronExpr":"* * * * * * *"}
func handleJobSave(resp http.ResponseWriter, req *http.Request)  {
	var (
		err     error
		postJob string
		job     common.Job
		oldJob  *common.Job
		bytes   []byte
	)

	// 解析POST表单
	if err = req.ParseForm(); err != nil{
		goto ERR
	}

	// 取表单中的job字段
	postJob = req.PostForm.Get("job")

	// 反序列化job
	if err = json.Unmarshal([]byte(postJob), &job); err != nil{
		goto ERR
	}

	// 保存到etcd
	if oldJob,err = G_jobMgr.SaveJob(&job); err != nil{
		goto ERR
	}

	// 返回正常应答 ({"errno":0,"msg":"","data":{...}})
	if bytes,err = common.BuildResponse(0, "success", oldJob); err == nil{
		resp.Write(bytes)
	}

	return
ERR:
	// 返回异常应答
	if bytes,err = common.BuildResponse(-1, err.Error(), nil); err == nil{
		resp.Write(bytes)
	}
}

// 删除任务接口
// POST /job/delete name=job1
func handleJobDelete(resp http.ResponseWriter, req *http.Request) {
	var(
		err error
		name string
		oldJob *common.Job
		bytes []byte
	)

	// POST: a=1&b=2&c=3
	if err = req.ParseForm(); err != nil{
		goto ERR
	}

	// 删除的任务名
	name = req.PostForm.Get("name")

	// 去删除任务
	if oldJob, err = G_jobMgr.DeleteJob(name); err != nil{
		goto ERR
	}

	// 返回正常应答 ({"errno":0,"msg":"","data":{...}})
	if bytes,err = common.BuildResponse(0, "success", oldJob); err == nil{
		resp.Write(bytes)
	}

	return
ERR:
	// 返回异常应答
	if bytes,err = common.BuildResponse(-1, err.Error(), nil); err == nil{
		resp.Write(bytes)
	}
}

// 列取所有crontab任务
func handleJobList(resp http.ResponseWriter, req *http.Request)  {
	var (
		jobList []*common.Job
		bytes []byte
		err error
	)

	// 获取任务列表
	if jobList,err = G_jobMgr.ListJobs(); err != nil{
		goto ERR
	}

	// 返回正常应答 ({"errno":0,"msg":"","data":{...}})
	if bytes,err = common.BuildResponse(0, "success", jobList); err == nil{
		resp.Write(bytes)
	}

	return
ERR:
	// 返回异常应答
	if bytes,err = common.BuildResponse(-1, err.Error(), nil); err == nil{
		resp.Write(bytes)
	}
}

// 强制杀死某个任务
// POST /job/killer name=job1
func handleJobKill(resp http.ResponseWriter, req *http.Request)  {
	var (
		name string
		bytes []byte
		err error
	)

	// 解析POST表单
	if err = req.ParseForm(); err != nil{
		goto ERR
	}

	// 要杀死的任务名
	name = req.PostForm.Get("name")

	// 杀死任务
	if err = G_jobMgr.KillJob(name); err != nil{
		goto ERR
	}

	// 返回正常应答 ({"errno":0,"msg":"","data":{...}})
	if bytes,err = common.BuildResponse(0, "success", nil); err == nil{
		resp.Write(bytes)
	}

	return
ERR:
	// 返回异常应答
	if bytes,err = common.BuildResponse(-1, err.Error(), nil); err == nil{
		resp.Write(bytes)
	}
}

// 查询任务日志
func handleJobLog(resp http.ResponseWriter, req *http.Request)  {
	var (
		err error
		name string // 任务名称
		skipParam string // 从第几条开始
		limitParam string // 返回多少条
		skip int64
		limit int64
		logArr []*common.JobLog
		bytes []byte
	)

	if err = req.ParseForm(); err != nil{
		goto ERR
	}

	// 获取请求参数 /job/log?name=job10&skip=0&limit=10
	name = req.Form.Get("name")
	skipParam = req.Form.Get("skip")
	limitParam = req.Form.Get("limit")

	if skip, err = strconv.ParseInt(skipParam,10,64);err != nil{
		skip = 0
	}

	if limit, err = strconv.ParseInt(limitParam,10,64); err != nil{
		limit = 20
	}


	if logArr, err = G_logMgr.ListLog(name, skip, limit); err != nil{
		goto ERR
	}

	// 返回正常应答 ({"errno":0,"msg":"","data":{...}})
	if bytes,err = common.BuildResponse(0, "success", logArr); err == nil{
		resp.Write(bytes)
	}

	return
ERR:
	// 返回异常应答
	if bytes,err = common.BuildResponse(-1, err.Error(), nil); err == nil{
		resp.Write(bytes)
	}
}

// 获取健康worker节点列表
func handleWorkerlist(resp http.ResponseWriter, req *http.Request) {
	var (
		workerArr []string
		err error
		bytes []byte
	)

	if workerArr, err = G_workerMgr.ListWorkers(); err != nil{
		goto ERR
	}

	// 返回正常应答 ({"errno":0,"msg":"","data":{...}})
	if bytes,err = common.BuildResponse(0, "success", workerArr); err == nil{
		resp.Write(bytes)
	}

	return
ERR:
	// 返回异常应答
	if bytes,err = common.BuildResponse(-1, err.Error(), nil); err == nil{
		resp.Write(bytes)
	}
}

// 初始化服务
func InitApiServer() (err error) {
	var (
		mux *http.ServeMux
		listener net.Listener
		httpServer *http.Server
		staticDir http.Dir // 静态文件根目录
		staticHandler http.Handler // 静态文件的HTTP回调
	)

	// 配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)
	mux.HandleFunc("/job/log", handleJobLog)
	mux.HandleFunc("/worker/list", handleWorkerlist)

	// 静态文件目录
	staticDir = http.Dir(G_config.WebRoot)
	staticHandler = http.FileServer(staticDir)
	mux.Handle("/", http.StripPrefix("/", staticHandler))
	// /index.html StripPrefix 会将prefix /去掉，即index.html（strip掉url的前缀），然后再转发给staticHandler
	// staticHandler就是去staticDir中找到./webroot/index.html

	// 启动TCP监听
	if listener,err = net.Listen("tcp", ":" + strconv.Itoa(G_config.ApiPort)); err != nil{
		return err
	}

	// 创建一个HTTP服务
	httpServer = &http.Server{
		ReadTimeout:time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout:time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler:mux,
	}
	// 这里定义的Handler和HandleFunc中的handleJobSave是同一类型的回调函数，它们的参数是一样的
	// 所谓的路由其实也是一个Handler，它无非是帮助我们转发一下请求，当http.Server收到请求之后，会回调Handler方法，传入请求
	// http.Server中的Handler，我们定义是mux路由，路由接收到请求之后，路由内部会根据请求的url，遍历它自己的路由表，找到那个匹配的回调函数，再将请求转发给该匹配的回调函数
	// 路由其实是一个代理模式

	// 赋值单例
	G_apiServer = &ApiServer{
		httpServer:httpServer,
	}

	// 启动服务端
	go httpServer.Serve(listener)

	return err
}

