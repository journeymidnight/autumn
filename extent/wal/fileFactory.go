/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless  by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package wal

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/journeymidnight/autumn/extent/storage"
	"github.com/journeymidnight/autumn/utils"
)

type fileFactory struct {
	dir     string
	output  chan storage.File
	count   int
	stopper *utils.Stopper
	errCh   chan error
}

func (ff *fileFactory) Close() {
	ff.stopper.Close()
}

func (ff *fileFactory) Get() (storage.File, error) {
	select {
	case err := <-ff.errCh:
		return nil, err
	case f := <-ff.output:
		return f, nil
	}
}

func NewFileFactory(dir string) *fileFactory {
	ff := &fileFactory{
		dir:     dir,
		output:  make(chan storage.File),
		errCh:   make(chan error),
		stopper: utils.NewStopper(),
	}

	ff.stopper.RunWorker(func() {
		defer close(ff.errCh)
		count := 0
		for {
			fname := filepath.Join(ff.dir, fmt.Sprintf("%016x.wal", count%2))
			tmpFile, err := storage.Default.Create(fname)
			if err != nil {
				ff.errCh <- err
				return
			}

			count++
			select {
			case ff.output <- tmpFile:
				break
			case <-ff.stopper.ShouldStop():
				tmpFile.Close()
				os.Remove(fname)
				return
			}
		}

	})
	return ff
}
