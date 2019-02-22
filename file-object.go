// Copyright 2018 Eryx <evorui аt gmail dοt com>, All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package localfs // import "github.com/lynkdb/localfs"

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/lynkdb/iomix/connect"
	"github.com/lynkdb/iomix/skv"
)

type FileObjectConnector struct {
	opts foOptions
}

type foOptions struct {
	DataDir string `json:"data_dir,omitempty"`
}

func FileObjectConnect(copts connect.ConnOptions) (*FileObjectConnector, error) {

	var (
		opts = foOptions{}
	)

	if v, ok := copts.Items.Get("data_dir"); ok {
		opts.DataDir = filepath.Clean(v.String())
	} else {
		return nil, errors.New("No Storage Dir Found")
	}

	if stat, err := os.Stat(opts.DataDir); err != nil {
		return nil, fmt.Errorf("Storage Dir (%s) Not Found", opts.DataDir)
	} else if !stat.IsDir() {
		return nil, fmt.Errorf("Storage Dir (%s) Is Not Dir", opts.DataDir)
	}

	return &FileObjectConnector{
		opts: opts,
	}, nil
}

func (conn *FileObjectConnector) FoFileOpen(path string) (io.ReadSeeker, error) {

	path = filepath.Clean(path)

	if path == "" || path == "." || path == ".." {
		return nil, errors.New("Invalid Path")
	}

	fp, err := os.Open(conn.opts.DataDir + "/" + filepath.Clean(path))
	if err != nil {
		return nil, err
	}

	return &FsObject{
		fp: fp,
	}, nil
}

func (conn *FileObjectConnector) FoFilePut(src_path string, dst_path string) skv.Result {

	src_path = filepath.Clean(src_path)
	if src_path == "" || src_path == "." || src_path == ".." {
		return newResult(skv.ResultError, errors.New("Invalid SRC Path"))
	}

	dst_path = filepath.Clean(conn.opts.DataDir + "/" + dst_path)
	if dst_path == "" || dst_path == "." || dst_path == ".." {
		return newResult(skv.ResultError, errors.New("Invalid DST Path"))
	}

	if dst_dir := filepath.Dir(dst_path); dst_dir != "" {
		if err := os.MkdirAll(dst_dir, 0755); err != nil {
			return newResult(skv.ResultError, err)
		}
	}

	fpsrc, err := os.Open(src_path)
	if err != nil {
		return newResult(skv.ResultError, err)
	}
	defer fpsrc.Close()

	sts, err := fpsrc.Stat()
	if err != nil {
		return newResult(skv.ResultError, err)
	}
	if sts.Size() < 1 {
		return newResult(skv.ResultError, errors.New("zero size"))
	}

	fpdst, err := os.OpenFile(dst_path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return newResult(skv.ResultError, err)
	}
	defer fpdst.Close()
	fpdst.Seek(0, 0)
	fpdst.Truncate(0)

	if _, err := io.Copy(fpdst, fpsrc); err != nil {
		return newResult(skv.ResultError, err)
	}

	if err := fpdst.Sync(); err != nil {
		return newResult(skv.ResultError, err)
	}

	return newResult(skv.ResultOK, nil)
}

func (conn *FileObjectConnector) Close() error {
	return nil
}
