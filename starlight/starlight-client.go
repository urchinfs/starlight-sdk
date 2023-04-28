package starlight

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	logger "github.com/urchinfs/starlight-sdk/dflog"
	"github.com/urchinfs/starlight-sdk/util"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Starlightclient interface {

	// get file list for path.
	GetFileList(path string, showHidden bool) ([]FileMeta, error)

	// check file or directory exist.
	FileExist(path string) (bool, error)

	// create dir for given path, auto create parent dir if not exist.
	CreateDir(path string) (bool, error)

	// delete dir or file for given path, recursively children dirs if exist.
	DeleteFile(path string) (bool, error)
	// get file meta for given file
	GetFileMeta(path string) (*FileMeta, error)

	// download file or directory
	Download(path, tmpDir string) (io.ReadCloser, error)

	// upload
	Upload(filePath string, reader io.Reader, totalLength int64) error

	// upload tinyfile
	UploadTinyFile(filePath string, reader io.Reader) error

	// upload bigfile
	UploadBigFile(filePath string, reader io.Reader, totalLength int64) error

	// get sign url
	GetSignURL(path string) string

	GetFileListRecursive(path string, showHidden bool, maxDepth int) ([]FileMeta, error)
}

/*
	homeDir is the base dir for specific user account, since starlight https api doesn't provide default dir param for dir filelist
	lustreType is the filesystem type, like WORK、BIGDATA2、GPUFS
*/

type starlightclient struct {
	token         string
	lustreType    string
	username      string
	password      string
	apiEnv        string
	tokenCreateAt time.Time
}

func New(lustreType, username, password, apiEnv string) (Starlightclient, error) {
	return &starlightclient{
		lustreType: lustreType,
		username:   username,
		password:   password,
		apiEnv:     apiEnv,
	}, nil
}

type StarlightResp struct {
	UUID  string `json:"uuid"`
	Code  int    `json:"code"`
	Info  string `json:"info"`
	Kind  string `json:"kind"`
	Total int    `json:"total"`
	Spec  string `json:"spec"`
}

type FileMeta struct {
	Name string `json:"name"`
	Path string `json:"path"`
	Size int    `json:"size"`
	Type int    `json:"type"`
	Perm string `json:"perm"`
	Time string `json:"time"`
	UID  int    `json:"uid"`
	Gid  int    `json:"gid"`
}

type FileMetaResp struct {
	UUID  string   `json:"uuid"`
	Code  int      `json:"code"`
	Info  string   `json:"info"`
	Kind  string   `json:"kind"`
	Total int      `json:"total"`
	Spec  FileMeta `json:"spec"`
}

type FileListResp struct {
	UUID  string     `json:"uuid"`
	Code  int        `json:"code"`
	Info  string     `json:"info"`
	Kind  string     `json:"kind"`
	Total int        `json:"total"`
	Spec  []FileMeta `json:"spec"`
}

type UploadResp struct {
	UUID  string `json:"uuid"`
	Code  int    `json:"code"`
	Info  string `json:"info"`
	Kind  string `json:"kind"`
	Total int    `json:"total"`
	Spec  struct {
		File    string `json:"File"`
		Written int    `json:"Written"`
	} `json:"spec"`
}

const (
	CHUNK_SIZE          = 16 * 1024 * 1024
	READ_BUFFER_SIZE    = 32 * 1024
	FILE_SHARD_LIMIT    = 128 * 1024 * 1024
	DOWNLOAD_SHARD_SIZE = 500 * 1024 * 1024
)

func (sl *starlightclient) isTokenValid() (bool, error) {
	if sl.token == "" || sl.tokenCreateAt.IsZero() {
		return false, nil
	}

	if time.Now().Sub(sl.tokenCreateAt).Hours() > 12 {
		logger.Infof("starlight&&&token expires 12 hours , need update")
		return false, nil
	}

	return true, nil
}

func (sl *starlightclient) SetToken() error {
	//生成client 参数为默认
	valid, _ := sl.isTokenValid()
	if valid {
		return nil
	}

	anyResponse, _, err := util.Run(15, 100, 4, "GetFileList", func() (any, bool, error) {

		url := sl.apiEnv + "/keystone/short_term_token/name"
		method := "POST"
		payload := strings.NewReader("{\"username\": \"" + sl.username + "\",\"password\": \"" + sl.password + "\"}")

		client := &http.Client{}
		req, err := http.NewRequest(method, url, payload)

		if err != nil {
			logger.Errorf("starlight---SetToken: " + err.Error())
			return nil, false, err
		}
		req.Header.Add("Content-Type", "application/json")

		response, err := client.Do(req)

		if err != nil {
			logger.Errorf("starlight---SetToken: " + err.Error())
			return nil, false, err
		}

		if response == nil {
			logger.Errorf("starlight---SetToken response nil error")
			return nil, false, fmt.Errorf("starlight---response nil")
		}

		if response.StatusCode/100 != 2 {
			err = fmt.Errorf("starlight---SetToken request responseCode %s", response.StatusCode)
		}
		return response, false, err
	})

	if err != nil {
		logger.Errorf("starlight---SetToken request error %s", err.Error())
		return err
	}
	response := anyResponse.(*http.Response)

	if err != nil {
		logger.Errorf("starlight---SetToken: " + err.Error())
		return err
	}
	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	var starlightResp StarlightResp
	if err != nil {
		return err
	}
	err = json.Unmarshal((body), &starlightResp)
	if err != nil {
		logger.Errorf("starlight---GetFileMeta json parse error " + err.Error())
		return err
	}
	if starlightResp.Code != 200 {
		return fmt.Errorf("starlight---SetToken bad token return code %s %s", starlightResp.Code, starlightResp.Info)
	}
	sl.token = starlightResp.Spec
	sl.tokenCreateAt = time.Now()
	return nil

}

func (sl *starlightclient) GetFileList(path string, showHidden bool) ([]FileMeta, error) {
	err := sl.SetToken()
	if err != nil {
		logger.Errorf("starlight---GetFileList file=%s setToken error=%s", path, err.Error())
		return nil, err
	}
	if path == "" {
		path = "/" + sl.lustreType + "/" + sl.username
	}

	anyResponse, _, err := util.Run(15, 100, 4, "GetFileList", func() (any, bool, error) {

		response, err := util.LoopDoRequest(func() (*http.Response, error) {
			requestUrl := sl.apiEnv + "/storage/dir_info"
			data := make(url.Values)
			data["dir"] = []string{path}
			data["show_hidden"] = []string{strconv.FormatBool(showHidden)}
			uri, _ := url.Parse(requestUrl)
			values := uri.Query()
			if values != nil {
				for k, v := range values {
					data[k] = v
				}
			}
			uri.RawQuery = data.Encode()
			//提交请求
			request, err := http.NewRequest(http.MethodGet, uri.String(), nil)
			if err != nil {
				logger.Errorf("starlight---GetFileList request error " + err.Error())
				return nil, err
			}
			request.Header.Add("bihu-token", sl.token)

			client := &http.Client{}
			defer client.CloseIdleConnections()
			//处理返回结果
			return client.Do(request)
		})

		if err != nil {
			return nil, false, err
		}

		if response == nil {
			logger.Errorf("starlight---GetFileList response nil error")
			return nil, false, fmt.Errorf("starlight---response nil")
		}

		if response.StatusCode/100 != 2 {
			err = fmt.Errorf("starlight---GetFileList path=%s request responseCode %s", path, response.StatusCode)
		}
		return response, false, err
	})

	if err != nil {
		logger.Errorf("starlight---GetFileMeta path=%s request error %s", path, err.Error())
		return nil, err
	}
	response := anyResponse.(*http.Response)
	if response.StatusCode/100 != 2 {
		return nil, fmt.Errorf("starlight---GetFileMeta path=%s StatusCode error %s", path, response.StatusCode)
	}

	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	var fileListResp FileListResp
	err = json.Unmarshal((body), &fileListResp)
	if err != nil {
		return nil, err
	}

	if fileListResp.Code == 200 {
		return fileListResp.Spec, nil
	}
	return nil, fmt.Errorf("NoSuchKey")
}

func (sl *starlightclient) GetAllFile(path string, showHidden bool, fileList *[]FileMeta, depth, maxDepth int) error {
	if depth >= maxDepth {
		return fmt.Errorf("starlight---dir depth exceed maxDepth error depth=%d maxDepth=%d", depth, maxDepth)
	}
	files, err := sl.GetFileList(path, showHidden)
	if err != nil {
		logger.Errorf("starlight---GetAllFile GetFileList error %s", err.Error())
		return err
	}
	for _, file := range files {
		if file.Type == 1 {
			sl.GetAllFile(file.Path, showHidden, fileList, depth+1, maxDepth)
		} else {
			*fileList = append(*fileList, file)
		}
	}
	dirMeta, err := sl.GetFileMeta(path)
	if err != nil {
		logger.Errorf("starlight---GetAllFile GetFileMeta error %s", err.Error())
		return err
	}
	*fileList = append(*fileList, *dirMeta)
	return nil
}

func (sl *starlightclient) GetFileListRecursive(path string, showHidden bool, maxDepth int) ([]FileMeta, error) {
	var fileList []FileMeta
	err := sl.GetAllFile(path, showHidden, &fileList, 0, maxDepth)
	return fileList, err
}

func (sl *starlightclient) GetFileMeta(file string) (*FileMeta, error) {
	err := sl.SetToken()
	if err != nil {
		logger.Errorf("starlight---GetFileMeta file=%s setToken error=%s", file, err.Error())
		return nil, err
	}

	fileMeta, _, err := util.Run(15, 100, 4, "GetFileMeta", func() (any, bool, error) {

		response, err := util.LoopDoRequest(func() (*http.Response, error) {
			requestUrl := sl.apiEnv + "/storage/state"
			data := make(url.Values)
			data["file"] = []string{file}
			uri, _ := url.Parse(requestUrl)
			values := uri.Query()
			if values != nil {
				for k, v := range values {
					data[k] = v
				}
			}
			uri.RawQuery = data.Encode()
			//提交请求
			request, err := http.NewRequest(http.MethodGet, uri.String(), nil)
			if err != nil {
				logger.Errorf("starlight---GetFileMeta file=%s request error %s", file, err.Error())
				return nil, err
			}
			request.Header.Add("bihu-token", sl.token)

			client := &http.Client{}
			defer client.CloseIdleConnections()
			//处理返回结果
			return client.Do(request)
		})

		if err != nil {
			return nil, false, err
		}

		if response == nil {
			logger.Errorf("starlight---FileOperate response nil Error")
			return nil, false, fmt.Errorf("starlight---response nil")
		}

		if response.StatusCode/100 != 2 {
			err = fmt.Errorf("starlight---GetFileMeta file=%s request responseCode %s", file, response.StatusCode)
		}
		defer response.Body.Close()
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			logger.Errorf("starlight---GetFileMeta file=%s GetFileMeta body read error %s", file, err.Error())
			return nil, false, err
		}
		var fileMetaResp FileMetaResp
		err = json.Unmarshal((body), &fileMetaResp)
		if err != nil {
			logger.Errorf("starlight---GetFileMeta file=%s GetFileMeta json parse error %s", file, err.Error())
			return nil, false, err
		}

		if fileMetaResp.Code == 200 {
			return &fileMetaResp.Spec, true, nil
		} else if fileMetaResp.Code == 11502 {
			return nil, true, fmt.Errorf("NoSuchKey")
		} else {
			logger.Errorf("fileMetaResp.Code %s", fileMetaResp.Code)
			return nil, false, fmt.Errorf("starlight---GetFileMeta error fileMetaResp.Code=%s fileMetaResp.Info=%s", fileMetaResp.Code, fileMetaResp.Info)
		}
	})

	if err != nil {
		return nil, err
	}
	return fileMeta.(*FileMeta), err

}

func (sl *starlightclient) FileExist(file string) (bool, error) {
	_, err := sl.GetFileMeta(file)
	if err != nil && strings.Contains(err.Error(), "NoSuchKey") {
		return false, nil
	} else if err == nil {
		return true, nil
	} else {
		return false, err
	}
}

func (sl *starlightclient) FileOperate(opt, from, target, recursive, force string) (bool, error) {
	err := sl.SetToken()
	if err != nil {
		return false, err
	}

	requestUrl := sl.apiEnv + "/storage/operation"

	data := make(url.Values)
	data["opt"] = []string{opt}
	data["from"] = []string{from}
	data["target"] = []string{target}
	data["recursive"] = []string{recursive}
	data["force"] = []string{force}
	uri, _ := url.Parse(requestUrl)
	values := uri.Query()
	if values != nil {
		for k, v := range values {
			data[k] = v
		}
	}
	uri.RawQuery = data.Encode()
	anyResponse, _, err := util.Run(15, 100, 4, "FileOperate-"+opt, func() (any, bool, error) {
		response, err := util.LoopDoRequest(func() (*http.Response, error) {
			//提交请求
			request, err := http.NewRequest(http.MethodPost, uri.String(), nil)
			if err != nil {
				logger.Errorf("starlight---FileOperate opt=%s target=%s request error %s", opt, target, err.Error())
				return nil, err
			}
			request.Header.Add("bihu-token", sl.token)

			client := &http.Client{}
			defer client.CloseIdleConnections()
			//处理返回结果
			return client.Do(request)
		})
		if err != nil {
			return nil, false, err
		}

		if response == nil {
			logger.Errorf("starlight---FileOperate response nil Error")
			return nil, false, fmt.Errorf("starlight---response nil")
		}

		if response.StatusCode/100 != 2 {
			err = fmt.Errorf("starlight---FileOperate opt=%s bad resp status %s", opt, response.StatusCode)
		}
		return response, false, err
	})

	if err != nil {
		logger.Errorf("starlight---FileOperate Error %s", err)
		return false, err
	}
	response := anyResponse.(*http.Response)
	if response.StatusCode/100 != 2 {
		return false, fmt.Errorf("starlight---FileOperate opt=%s target=%s StatusCode error %d", opt, target, response.StatusCode)
	}

	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		logger.Errorf("starlight---FileOperate opt=%s target=%s body read error %s", opt, target, err.Error())
		return false, err
	}
	var starlightResp StarlightResp
	err = json.Unmarshal((body), &starlightResp)
	if err != nil {
		return false, err
	}

	if starlightResp.Code != 200 {
		return false, fmt.Errorf("starlight FileOperate opt=%s target=%s data code error Code=%d Info=%s", opt, target, starlightResp.Code, starlightResp.Info)
	}

	return true, nil
}

func (sl *starlightclient) CreateDir(path string) (bool, error) {

	return sl.FileOperate("mkdir", "", path, "true", "true")

}

func (sl *starlightclient) DeleteFile(path string) (bool, error) {

	return sl.FileOperate("rm", "", path, "true", "true")

}

func (sl *starlightclient) Download(path, tmpDir string) (io.ReadCloser, error) {
	fileMeta, err := sl.GetFileMeta(path)
	if err != nil {
		return nil, err
	}
	if fileMeta.Size < DOWNLOAD_SHARD_SIZE {
		return sl.DownloadTinyFile(path)
	} else {
		return sl.DownloadByShard(path, tmpDir, fileMeta)
	}
}

func (sl *starlightclient) DownloadTinyFile(path string) (io.ReadCloser, error) {
	err := sl.SetToken()
	if err != nil {
		return nil, err
	}

	//处理返回结果
	anyResponse, _, err := util.Run(15, 100, 4, "Download", func() (any, bool, error) {
		response, err := util.LoopDoRequest(func() (*http.Response, error) {
			//生成要访问的url
			requestUrl := sl.apiEnv + "/storage/download"
			data := make(url.Values)
			data["file"] = []string{path}
			uri, _ := url.Parse(requestUrl)
			values := uri.Query()
			if values != nil {
				for k, v := range values {
					data[k] = v
				}
			}
			uri.RawQuery = data.Encode()
			//提交请求
			request, err := http.NewRequest(http.MethodGet, uri.String(), nil)
			if err != nil {
				return nil, err
			}
			request.Header.Add("bihu-token", sl.token)
			client := &http.Client{}
			defer client.CloseIdleConnections()
			return client.Do(request)
		})
		if err != nil {
			return nil, false, err
		}

		if response == nil {
			logger.Errorf("starlight---Download response nil Error")
			return nil, false, fmt.Errorf("starlight---response nil")
		}

		if response.StatusCode/100 != 2 {
			err = fmt.Errorf("starlight---Download bad resp status %s", response.StatusCode)
		}
		return response, false, err
	})

	if err != nil {
		logger.Errorf("starlight---Download Error %s", err)
		return nil, err
	}
	response := anyResponse.(*http.Response)

	return response.Body, nil
}

type FileOpenCloser struct {
	reader   io.Reader
	tmpFile  *os.File
	filePath string
}

func (foc FileOpenCloser) Read(p []byte) (n int, err error) {
	return foc.reader.Read(p)
}

func (foc FileOpenCloser) Close() error {
	err := foc.tmpFile.Close()
	if err != nil {
		logger.Errorf("starlight---Close tmpFile error=%s", err.Error())
		return err
	}
	err = os.Remove(foc.filePath)
	if err != nil {
		logger.Errorf("starlight---Auto Delete tmpFile error=%s", err.Error())
		return err
	}
	return nil
}

func (sl *starlightclient) DownloadByShard(path, tmpDir string, fileMeta *FileMeta) (io.ReadCloser, error) {
	err := sl.SetToken()
	if err != nil {
		return nil, err
	}

	// check
	if _, err := os.Stat(tmpDir); err == nil {
		logger.Infof("DownloadByShard&&&tmpDir exist %s", tmpDir)
	} else {
		logger.Infof("DownloadByShard&&&tmpDir not exist %s try to create", tmpDir)
		err := os.MkdirAll(tmpDir, 0711)
		if err != nil {
			tmpDir = ""
			logger.Errorf("DownloadByShard&&&tmpDir failed to create tmpDir %s, use current dir as tmpDir", tmpDir)
		}
	}

	// 计算分片的数量
	fileLength := int64(fileMeta.Size)
	numChunks := (fileLength + DOWNLOAD_SHARD_SIZE - 1) / DOWNLOAD_SHARD_SIZE
	//生成要访问的url
	requestUrl := sl.apiEnv + "/storage/download"
	data := make(url.Values)
	data["file"] = []string{path}
	uri, _ := url.Parse(requestUrl)
	values := uri.Query()
	if values != nil {
		for k, v := range values {
			data[k] = v
		}
	}
	uri.RawQuery = data.Encode()

	tmpFilePath := filepath.Join(tmpDir, fileMeta.Name)
	tmpFile, err := os.Create(tmpFilePath)
	if err != nil {
		panic(err)
	}
	for i := int64(0); i < numChunks; i++ {

		start := i * DOWNLOAD_SHARD_SIZE
		end := start + DOWNLOAD_SHARD_SIZE - 1
		if end >= fileLength {
			end = fileLength - 1
		}

		//分片有限次重试缓解连接端开问题
		_, _, err = util.Run(15, 100, 4, "Download", func() (any, bool, error) {

			// 部分多次重试解决限流问题
			response, err := util.LoopDoRequest(func() (*http.Response, error) {
				client := &http.Client{}
				defer client.CloseIdleConnections()
				request, err := http.NewRequest(http.MethodGet, uri.String(), nil)

				if err != nil {
					logger.Errorf("starlight---DownloadByShard new request error", err.Error())
					return nil, err
				}
				request.Header.Add("User-Agent", "Apifox/1.0.0 (https://www.apifox.cn)")
				request.Header.Add("bihu-token", sl.token)
				request.Header.Add("Range", "bytes="+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10))
				response, err := client.Do(request)
				if err != nil {
					logger.Errorf("starlight---DownloadByShard do request", err.Error())
					return nil, err
				}
				return response, err
			})

			if err != nil {
				return nil, false, err
			}

			if response == nil {
				logger.Errorf("starlight---Download response nil Error")
				return nil, false, fmt.Errorf("starlight---DownloadByShard response nil")
			}

			if response.StatusCode/100 != 2 {
				err = fmt.Errorf("starlight---Download bad resp status %s", response.StatusCode)
			}
			copyLen, err := io.Copy(tmpFile, response.Body)
			logger.Infof("starlight&&&DownloadByShard shard copyLen=%d", copyLen)
			if err != nil {
				logger.Errorf("starlight---DownloadByShard read all err=%s", err.Error())
				return nil, false, err
			}
			defer response.Body.Close()
			return nil, false, err
		})
		if err != nil {
			logger.Errorf("starlight---DownloadByShard shard err=%s", err.Error())
			return nil, err
		}
	}
	tmpFile.Close()
	tmpFile, err = os.Open(tmpFilePath)
	if err != nil {
		logger.Errorf("starlight---DownloadByShard reopen file err=%s", err.Error())
		log.Fatal(err)
	}
	reader := bufio.NewReader(tmpFile)

	if err != nil {
		logger.Errorf("starlight---DownloadByShard Error %s", err)
		return nil, err
	}

	return FileOpenCloser{tmpFile: tmpFile, filePath: tmpFilePath, reader: reader}, err

}

func (sl *starlightclient) Upload(filePath string, reader io.Reader, totalLength int64) error {
	if totalLength < FILE_SHARD_LIMIT {
		logger.Infof("starlight&&&start upload by UploadTinyFile %dB", totalLength)
		return sl.UploadTinyFile(filePath, reader)
	} else {
		logger.Infof("starlight&&&start upload by UploadBigFile %dB", totalLength)
		return sl.UploadBigFile(filePath, reader, totalLength)
	}
}

func (sl *starlightclient) UploadTinyFile(filePath string, reader io.Reader) error {
	err := sl.SetToken()
	if err != nil {
		return err
	}

	_, _, err = util.Run(15, 100, 4, "UploadTinyFile", func() (any, bool, error) {

		response, err := util.LoopDoRequest(func() (*http.Response, error) {
			var buf = bytes.Buffer{}
			tee := io.TeeReader(reader, &buf)
			io.ReadAll(tee)
			//生成要访问的url
			requestUrl := sl.apiEnv + "/storage/upload"
			data := make(url.Values)
			data["file"] = []string{filePath}
			data["overwrite"] = []string{"true"}
			uri, _ := url.Parse(requestUrl)
			values := uri.Query()
			if values != nil {
				for k, v := range values {
					data[k] = v
				}
			}
			uri.RawQuery = data.Encode()
			//提交请求
			request, err := http.NewRequest(http.MethodPut, uri.String(), &buf)
			if err != nil {
				return nil, err
			}
			request.Header.Add("bihu-token", sl.token)
			request.Header.Add("Content-Type", "text/plain")
			client := &http.Client{}
			defer client.CloseIdleConnections()
			return client.Do(request)
		})

		if err != nil {
			logger.Errorf("starlight---Upload TinyFile Error %s", err)
		}
		if response == nil {
			logger.Errorf("starlight---Upload TinyFile response nil Error")
			return nil, false, fmt.Errorf("starlight---response nil")
		}
		if response.StatusCode/100 != 2 {
			err = fmt.Errorf("starlight---Upload bad resp status %s", response.StatusCode)
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		var uploadResp UploadResp
		if err != nil {
			return nil, false, err
		}
		err = json.Unmarshal((body), &uploadResp)
		if err != nil {
			return nil, false, err
		}
		if uploadResp.Code != 200 {
			return nil, false, fmt.Errorf("starlight Upload failed, path=%s, Code=%s, Info=%s", filePath, uploadResp.Code, uploadResp.Info)
		}
		return nil, false, err
	})
	return err
}

func (sl *starlightclient) UploadBigFile(filePath string, reader io.Reader, totalLength int64) error {
	err := sl.SetToken()
	if err != nil {
		return err
	}

	currentOffset := 0
	var readLength int64 = 0
	for {
		n := 0
		dataBuffer := make([]byte, 0)
		pipeBuffer := make([]byte, READ_BUFFER_SIZE)
		for {
			nn, err := reader.Read(pipeBuffer)
			if err != nil && err != io.EOF {
				logger.Errorf("starlight---UploadBigFile %s", err.Error())
				return err
			}
			if nn == 0 {
				break
			}
			dataBuffer = append(dataBuffer, pipeBuffer[:nn]...)
			n += nn
			readLength += int64(nn)
			if readLength >= totalLength || n >= CHUNK_SIZE {
				break
			}
		}
		if n == 0 {
			break
		}

		length := n

		_, _, err = util.Run(15, 100, 4, "UploadBigFile", func() (any, bool, error) {

			response, err := util.LoopDoRequest(func() (*http.Response, error) {
				var buf = bytes.Buffer{}
				buf.Write(dataBuffer[:n])
				//tee := io.TeeReader(reader, &buf)
				//io.ReadAll(tee)

				//生成要访问的url
				requestUrl := sl.apiEnv + "/storage/upload"

				data := make(url.Values)
				data["file"] = []string{filePath}
				data["overwrite"] = []string{"true"}

				uri, _ := url.Parse(requestUrl)
				values := uri.Query()
				if values != nil {
					for k, v := range values {
						data[k] = v
					}
				}
				uri.RawQuery = data.Encode()
				//提交请求
				request, err := http.NewRequest(http.MethodPut, uri.String(), &buf)
				if err != nil {
					return nil, err
				}
				request.Header.Add("bihu-token", sl.token)
				request.Header.Add("Content-Range", "bytes="+strconv.Itoa(currentOffset)+"-"+strconv.Itoa(currentOffset+length-1)+"/"+strconv.FormatInt(totalLength, 10))
				client := &http.Client{}
				defer client.CloseIdleConnections()
				//处理返回结果
				return client.Do(request)
			})

			if response == nil {
				logger.Errorf("starlight---UploadBigFile response nil Error")
				return nil, false, fmt.Errorf("starlight---UploadBigFile response nil")
			}
			//logger.Infof("starlight***Upload error, bytes=%s, length=%s, n=%d, dataBuffer=%d, pipeBuffer=%d",
			//	strconv.Itoa(currentOffset)+"-"+strconv.Itoa(currentOffset+length-1)+"/"+strconv.FormatInt(totalLength, 10),
			//	strconv.Itoa(length), n, len(dataBuffer), len(pipeBuffer))

			if response.StatusCode/100 != 2 {
				err = fmt.Errorf("starlight---UploadBigFile bad resp status %s", response.StatusCode)
			}
			defer response.Body.Close()

			body, err := io.ReadAll(response.Body)
			if err != nil {
				logger.Errorf("starlight---UploadBigFile ReadAll error %s", err.Error())
				return nil, false, err
			}
			var uploadResp UploadResp
			err = json.Unmarshal((body), &uploadResp)
			if err != nil {
				logger.Errorf("starlight---UploadBigFile json parse %s", err.Error())
				return nil, false, err
			}
			if uploadResp.Code != 200 {
				logger.Errorf("starlight---Upload code=%s error=%s, bytes=%s, length=%s, n=%d, dataBuffer=%d, pipeBuffer=%d", uploadResp.Code, uploadResp.Info,
					strconv.Itoa(currentOffset)+"-"+strconv.Itoa(currentOffset+length-1)+"/"+strconv.FormatInt(totalLength, 10),
					strconv.Itoa(length), n, len(dataBuffer), len(pipeBuffer))
				return nil, false, fmt.Errorf("starlight---Upload failed, path=%s, Code=%s, Message=%s", filePath, uploadResp.Code, uploadResp.Info)
			}
			return nil, false, err
		})
		if err != nil {
			return err
		}
		currentOffset += length
	}
	return nil
}

func (sl *starlightclient) GetSignURL(path string) string {
	requestUrl := sl.apiEnv + "/storage/download"
	data := make(url.Values)
	data["file"] = []string{path}
	data["bihu-token"] = []string{sl.token}
	uri, _ := url.Parse(requestUrl)
	values := uri.Query()
	if values != nil {
		for k, v := range values {
			data[k] = v
		}
	}
	uri.RawQuery = data.Encode()
	return uri.String()
}
