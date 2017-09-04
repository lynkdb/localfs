// Copyright 2017 Eryx <evorui аt gmаil dοt cοm>, All rights reserved.
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
	"os"
	"path/filepath"
	"time"

	"github.com/lynkdb/iomix/connect"
	"github.com/lynkdb/iomix/fs"
)

type FsObject struct {
	fp *os.File
}

type FsObjectMeta struct {
	info os.FileInfo
}

type Connector struct {
	opts options
}

type options struct {
	DataDir string `json:"data_dir,omitempty"`
}

func Open(copts connect.ConnOptions) (*Connector, error) {

	var (
		opts = options{}
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

	/*
		fp, err := os.OpenFile(opts.DataDir, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			return nil, err
		}
		defer fp.Close()
	*/

	return &Connector{
		opts: opts,
	}, nil
}

func (conn *Connector) Stat(path string) (fs.FsObjectMeta, error) {

	path = filepath.Clean(path)

	if path == "" || path == "." || path == ".." {
		return nil, errors.New("Invalid Path")
	}

	stat, err := os.Stat(conn.opts.DataDir + "/" + filepath.Clean(path))
	if err != nil {
		return nil, err
	}

	return stat, nil
}

func (conn *Connector) MkdirAll(path string, perm os.FileMode) error {

	path = filepath.Clean(path)

	if path == "" || path == "." || path == ".." {
		return errors.New("Invalid Path")
	}

	return os.MkdirAll(conn.opts.DataDir+"/"+filepath.Clean(path), perm)
}

func (conn *Connector) Open(path string) (fs.FsObject, error) {

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

func (conn *Connector) OpenFile(path string, flag int, perm os.FileMode) (fs.FsObject, error) {

	path = filepath.Clean(path)

	if path == "" || path == "." || path == ".." {
		return nil, errors.New("Invalid Path")
	}

	fp, err := os.OpenFile(conn.opts.DataDir+"/"+filepath.Clean(path), flag, perm)
	if err != nil {
		return nil, err
	}

	return &FsObject{
		fp: fp,
	}, nil
}

func (conn *Connector) List(path string, limit int) ([]fs.FsObjectMeta, error) {

	path = filepath.Clean(path)

	if path == "" || path == "." || path == ".." {
		return nil, errors.New("Invalid Path")
	}

	fp, err := os.Open(conn.opts.DataDir + "/" + filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	defer fp.Close()

	rs, err := fp.Readdir(limit)
	if err != nil {
		return nil, err
	}

	ls := []fs.FsObjectMeta{}
	for _, v := range rs {
		ls = append(ls, v)
	}

	return ls, nil
}

func (conn *Connector) Close() error {
	return nil
}

func (fo *FsObject) Readdir(count int) ([]fs.FsObjectMeta, error) {
	return []fs.FsObjectMeta{}, nil
}

func (fo *FsObject) Stat() (fs.FsObjectMeta, error) {

	info, err := fo.fp.Stat()
	if err != nil {
		return nil, err
	}

	return &FsObjectMeta{
		info: info,
	}, nil
}

func (fo *FsObject) Seek(offset int64, whence int) (int64, error) {
	return fo.fp.Seek(offset, whence)
}

func (fo *FsObject) Read(b []byte) (n int, err error) {
	return fo.fp.Read(b)
}

func (fo *FsObject) Write(b []byte) (n int, err error) {
	return fo.fp.WriteAt(b, 0)
}

func (fo *FsObject) WriteAt(b []byte, off int64) (n int, err error) {
	return fo.fp.WriteAt(b, off)
}

func (fo *FsObject) Truncate(size int64) error {
	return fo.fp.Truncate(size)
}

func (fo *FsObject) Close() error {
	return fo.fp.Close()
}

func (foi *FsObjectMeta) Name() string {
	return foi.info.Name()
}

func (foi *FsObjectMeta) Size() int64 {
	return foi.info.Size()
}

func (foi *FsObjectMeta) IsDir() bool {
	return foi.info.IsDir()
}

func (foi *FsObjectMeta) ModTime() time.Time {
	return foi.info.ModTime()
}
