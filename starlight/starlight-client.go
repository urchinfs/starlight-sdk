package starlight

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	logger "github.com/urchinfs/starlight-sdk/dflog"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
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
	Download(path string) (io.ReadCloser, error)

	// upload
	Upload(filePath string, reader io.Reader, totalLength int64) error

	// upload tinyfile
	UploadTinyFile(filePath string, reader io.Reader) error

	// upload bigfile
	UploadBigFile(filePath string, reader io.Reader, totalLength int64) error

	// get sign url
	GetSignURL(path string) string
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
	CHUNK_SIZE       = 16 * 1024 * 1024
	READ_BUFFER_SIZE = 32 * 1024
	FILE_SHARD_LIMIT = 128 * 1024 * 1024
)

func (sl *starlightclient) isTokenValid() (bool, error) {
	if sl.token == "" || sl.tokenCreateAt.IsZero() {
		return false, nil
	}

	if time.Now().Sub(sl.tokenCreateAt).Hours() > 12 {
		logger.Infof("token expires 12 hours , need update")
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
	url := sl.apiEnv + "/keystone/short_term_token/name"
	method := "POST"
	payload := strings.NewReader("{\"username\": \"" + sl.username + "\",\"password\": \"" + sl.password + "\"}")

	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)

	if err != nil {
		logger.Errorf("starlight SetToken: " + err.Error())
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		logger.Errorf("starlight SetToken: " + err.Error())
		return err
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	var starlightResp StarlightResp
	if err != nil {
		return err
	}
	err = json.Unmarshal((body), &starlightResp)
	if err != nil {
		logger.Errorf("starlight GetFileMeta json parse error " + err.Error())
		return err
	}
	if starlightResp.Code != 200 {
		return fmt.Errorf("starlight SetToken bad token return code %s %s", starlightResp.Code, starlightResp.Info)
	}
	sl.token = starlightResp.Spec
	sl.tokenCreateAt = time.Now()
	println("token:" + sl.token)
	return fmt.Errorf("starlight SetToken get token failed")

}

func (sl *starlightclient) GetFileList(path string, showHidden bool) ([]FileMeta, error) {
	err := sl.SetToken()
	if err != nil {
		time.Sleep(time.Duration(1) * time.Second)
		err = sl.SetToken()
		if err != nil {
			return nil, err
		}
	}
	if path == "" {
		path = "/" + sl.lustreType + "/" + sl.username
	}

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
	logger.Infof("uri.String %s", uri.String())
	if err != nil {
		logger.Errorf("starlight GetFileList request error " + err.Error())
		return nil, err
	}
	request.Header.Add("bihu-token", sl.token)

	client := &http.Client{}
	//处理返回结果
	response, _ := client.Do(request)
	if response.StatusCode/100 != 2 {
		return nil, fmt.Errorf("starlight GetFileMeta path=%s StatusCode error %s", path, response.StatusCode)
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
	defer client.CloseIdleConnections()
	if fileListResp.Code == 200 {
		return fileListResp.Spec, nil
	}
	return nil, fmt.Errorf("404 NOT FOUND")
}

func (sl *starlightclient) GetFileMeta(file string) (*FileMeta, error) {
	err := sl.SetToken()
	if err != nil {
		time.Sleep(time.Duration(1) * time.Second)
		err = sl.SetToken()
		if err != nil {
			return nil, err
		}
	}

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
		logger.Errorf("starlight GetFileMeta file=%s request error %s", file, err.Error())
		return nil, err
	}
	request.Header.Add("bihu-token", sl.token)

	client := &http.Client{}
	//处理返回结果
	response, _ := client.Do(request)
	if response.StatusCode/100 != 2 {
		return nil, fmt.Errorf("starlight GetFileMeta file=%s StatusCode error %s", file, response.StatusCode)
	}

	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		logger.Errorf("starlight GetFileMeta file=%s GetFileMeta body read error %s", file, err.Error())
		return nil, err
	}
	var fileMetaResp FileMetaResp
	err = json.Unmarshal((body), &fileMetaResp)
	if err != nil {
		logger.Errorf("starlight GetFileMeta file=%s GetFileMeta json parse error %s", file, err.Error())
		return nil, err
	}
	defer client.CloseIdleConnections()
	if fileMetaResp.Code == 200 {
		return &fileMetaResp.Spec, nil
	} else if fileMetaResp.Code == 11502 {
		return nil, fmt.Errorf("404 NOT FOUND")
	}
	logger.Infof("fileMetaResp.Code %s", fileMetaResp.Code)
	return nil, fmt.Errorf("starlight GetFileMeta error fileMetaResp.Code=%s fileMetaResp.Info=%s", fileMetaResp.Code, fileMetaResp.Info)
}

func (sl *starlightclient) FileExist(file string) (bool, error) {
	_, err := sl.GetFileMeta(file)
	if err != nil && strings.Contains(err.Error(), "404") {
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
		time.Sleep(time.Duration(1) * time.Second)
		err = sl.SetToken()
		if err != nil {
			return false, err
		}
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

	//提交请求
	request, err := http.NewRequest(http.MethodPost, uri.String(), nil)
	if err != nil {
		logger.Errorf("starlight FileOperate opt=%s target=%s request error %s", opt, target, err.Error())
		return false, err
	}
	request.Header.Add("bihu-token", sl.token)

	client := &http.Client{}
	//处理返回结果
	response, _ := client.Do(request)
	if response.StatusCode/100 != 2 {
		return false, fmt.Errorf("starlight FileOperate opt=%s target=%s StatusCode error %d", opt, target, response.StatusCode)
	}

	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		logger.Errorf("starlight FileOperate opt=%s target=%s body read error %s", opt, target, err.Error())
		return false, err
	}
	fmt.Println(string(body))
	var starlightResp StarlightResp
	if err == nil {
		err = json.Unmarshal((body), &starlightResp)
	}
	defer client.CloseIdleConnections()
	if starlightResp.Code != 200 {
		return false, fmt.Errorf("starlight FileOperate opt=%s target=%s data code error Code=%d Info=%s", opt, target, starlightResp.Code, starlightResp.Info)
	}

	return true, nil
}

func (sl *starlightclient) CreateDir(path string) (bool, error) {

	return sl.FileOperate("mkdir", "", path, "true", "true")

}

func (sl *starlightclient) DeleteFile(path string) (bool, error) {

	return sl.FileOperate("rm", "", path, "false", "true")

}

func (sl *starlightclient) Download(path string) (io.ReadCloser, error) {
	err := sl.SetToken()
	if err != nil {
		time.Sleep(time.Duration(3) * time.Second)
		err = sl.SetToken()
		if err != nil {
			return nil, err
		}
	}

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
	logger.Infof("uri.String %s", uri.String())
	if err != nil {
		return nil, err
	}
	request.Header.Add("bihu-token", sl.token)
	client := &http.Client{}
	//处理返回结果
	response, _ := client.Do(request)
	if response.StatusCode/100 != 2 {
		return nil, fmt.Errorf("starlight---Download bad resp status=%s", response.StatusCode)
	}

	//fmt.Println(status)

	return response.Body, nil
}

func (sl *starlightclient) Upload(filePath string, reader io.Reader, totalLength int64) error {
	if totalLength < FILE_SHARD_LIMIT {
		logger.Infof("starlight start upload by UploadTinyFile %dB", totalLength)
		return sl.UploadTinyFile(filePath, reader)
	} else {
		logger.Infof("starlight start upload by UploadBigFile %dB", totalLength)
		return sl.UploadBigFile(filePath, reader, totalLength)
	}
}

func (sl *starlightclient) UploadTinyFile(filePath string, reader io.Reader) error {
	err := sl.SetToken()
	if err != nil {
		time.Sleep(time.Duration(1) * time.Second)
		err = sl.SetToken()
		if err != nil {
			return err
		}
	}

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
	//payload := strings.NewReader(`hello world`)
	//url := "https://starlight.nscc-gz.cn/api/storage/upload?file=/WORK/pcl_xcx_1/mnt&overwrite=true"
	println("uri.string: " + uri.String())
	//提交请求
	request, err := http.NewRequest(http.MethodPut, uri.String(), &buf)
	if err != nil {
		return err
	}
	request.Header.Add("bihu-token", sl.token)
	request.Header.Add("Content-Type", "text/plain")
	client := &http.Client{}
	//处理返回结果
	response, _ := client.Do(request)
	if response.StatusCode/100 != 2 {
		return fmt.Errorf("starlight---Upload bad resp status %s", response.StatusCode)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	var uploadResp UploadResp
	if err == nil {
		err = json.Unmarshal((body), &uploadResp)
	}

	defer client.CloseIdleConnections()
	if uploadResp.Code != 200 {
		return fmt.Errorf("starlight Upload failed, path=%s, Code=%s, Info=%s", filePath, uploadResp.Code, uploadResp.Info)
	}

	return nil
}

func (sl *starlightclient) UploadBigFile(filePath string, reader io.Reader, totalLength int64) error {
	err := sl.SetToken()
	if err != nil {
		time.Sleep(time.Duration(1) * time.Second)
		err = sl.SetToken()
		if err != nil {
			return err
		}
	}

	fileName := filepath.Base(filePath)

	currentOffset := 0
	for {
		n := 0
		dataBuffer := make([]byte, 0)
		pipeBuffer := make([]byte, READ_BUFFER_SIZE)
		for {
			nn, err := reader.Read(pipeBuffer)
			if err != nil && err != io.EOF {
				logger.Infof(err.Error())
				return err
			}
			if nn == 0 {
				break
			}
			dataBuffer = append(dataBuffer, pipeBuffer...)
			n += nn
			if nn < READ_BUFFER_SIZE || n >= CHUNK_SIZE {
				break
			}
		}
		if n == 0 {
			break
		}

		var buf = bytes.Buffer{}
		buf.Write(dataBuffer[:n])
		//tee := io.TeeReader(reader, &buf)
		//io.ReadAll(tee)

		bodyBuf := &bytes.Buffer{}
		bodyWriter := multipart.NewWriter(bodyBuf)
		fileWriter, err := bodyWriter.CreateFormFile("file", fileName)
		length, err := fileWriter.Write(dataBuffer[:n])
		if err != nil {
			return err
		}
		bodyWriter.Close()
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
			return err
		}
		request.Header.Add("bihu-token", sl.token)
		request.Header.Add("Content-Range", "bytes="+strconv.Itoa(currentOffset)+"-"+strconv.Itoa(currentOffset+length-1)+"/"+strconv.FormatInt(totalLength, 10))
		println("bytes:" + request.Header.Get("Content-Range") + " length:" + strconv.Itoa(length))
		currentOffset += length
		client := &http.Client{}
		//处理返回结果
		response, _ := client.Do(request)
		if response.StatusCode/100 != 2 {
			return fmt.Errorf("sugon---Upload bad resp status %s", response.StatusCode)
		}
		defer response.Body.Close()

		body, err := io.ReadAll(response.Body)
		var uploadResp UploadResp
		if err == nil {
			err = json.Unmarshal((body), &uploadResp)
		}
		defer client.CloseIdleConnections()
		if uploadResp.Code != 200 {
			return fmt.Errorf("starlight Upload failed, path=%s, Code=%s, Message=%s", filePath, uploadResp.Code, uploadResp.Info)
		}

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

func main() {

	starlightClient, err := New("WORK", "pcl_xcx_1", "UWl6aGkyMDIy", "https://starlight.nscc-gz.cn/api")
	if err != nil {
		panic(err)
	}
	//path := "/public/home/denglf"
	fileList, err := starlightClient.GetFileList("/WORK/pcl_xcx_1/denglf", true)
	if err != nil {
		panic(err)
	}
	for index, file := range fileList {
		fileTime, _ := time.ParseInLocation("2006-01-02 15:04:05", file.Time, time.Local)
		println(index, file.Name, fileTime.String())
	}

	file := "/WORK/pcl_xcx_1/log.lammps"
	fileExist, err := starlightClient.FileExist(file)
	if err != nil {
		panic(err)
	}
	print("fileExist:", fileExist)

	create_path := "/WORK/pcl_xcx_1/test_create"
	createSuccess, err := starlightClient.CreateDir(create_path)
	if err != nil {
		panic(err)
	}
	print("createSuccess:", createSuccess)

	create_path2 := "/WORK/pcl_xcx_1/test_create2"
	createSuccess2, err := starlightClient.CreateDir(create_path2)
	if err != nil {
		panic(err)
	}
	print("createSuccess:", createSuccess2)

	delete_path := "/WORK/pcl_xcx_1/_user_pcl_work_thu_news_input_json_text__user_pcl_work_thu_news_input_json_text_c4-validation.00000-of-00001.json"
	deleteSuccess, err := starlightClient.DeleteFile(delete_path)
	if err != nil {
		panic(err)
	}
	print("deleteSuccess:", deleteSuccess)

	filePath := "/WORK/pcl_xcx_1/log.lammps"
	fileMeta, err := starlightClient.GetFileMeta(filePath)
	if err != nil {
		panic(err)
	}
	println("fileMeta:", fileMeta.Name, " ", fileMeta.Size)

	f, err := os.Create("log.lammps")
	if err != nil {
		panic(err)
	}
	fileDownload, err := starlightClient.Download(filePath)
	if err != nil {
		panic(err)
	}
	defer fileDownload.Close()
	io.Copy(f, fileDownload)
	println("download success")

	uploadPath := "/Users/stardust/Downloads/_user_pcl_work_thu_news_input_json_text_c4-validation.00000-of-00001.json"
	uf, err := os.Open(uploadPath)
	if err != nil {
		fmt.Println("打开文件失败")
		log.Fatal(err)
	}
	reader := bufio.NewReader(uf)
	err = starlightClient.UploadTinyFile("/WORK/pcl_xcx_1/denglf/00001.json", reader)
	if err != nil {
		panic(err)
	}
	println("upload success")

	filePath = "/Users/stardust/Downloads/MicrosoftEdge-110.0.1587.41.pkg"
	content, err := os.Open(filePath)
	fileInfo, _ := os.Stat(filePath)
	err = starlightClient.UploadBigFile("/WORK/pcl_xcx_1/denglf/MicrosoftEdge-110.0.1587.41.pkg", content, fileInfo.Size())
	if err != nil {
		panic(err)
	}
	println("upload big file success")

}
