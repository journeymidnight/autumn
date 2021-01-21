/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package table

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"

	"github.com/journeymidnight/autumn/rangepartition/y"
	"github.com/journeymidnight/autumn/streamclient"
	"github.com/journeymidnight/autumn/xlog"
)

func init() {
	xlog.InitLog([]string{"test.log"}, zapcore.DebugLevel)
}
func TestTableIndex(t *testing.T) {

	rand.Seed(time.Now().Unix())
	keysCount := 100000
	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)

	//already have the cluster
	/*
		sm := smclient.NewSMClient([]string{"127.0.0.1:3401"})
		err = sm.Connect()
		assert.Nil(t, err)

		stream := streamclient.NewStreamClient(sm, 3, 32)
		err = stream.Connect()
		assert.Nil(t, err)
	*/

	stream := streamclient.NewMockStreamClient("test.tmp", 100)
	defer stream.Close()

	builder := NewTableBuilder(stream)

	blockFirstKeys := make([][]byte, 0)
	blockCount := 0
	for i := 0; i < keysCount; i++ {
		k := []byte(fmt.Sprintf("%016x", i))
		v := fmt.Sprintf("%d", i)
		vs := y.ValueStruct{Value: []byte(v)}
		if i == 0 { // This is first key for first block.
			blockFirstKeys = append(blockFirstKeys, k)
			blockCount = 1
		} else if builder.shouldFinishBlock(k, vs) {
			blockCount++
			blockFirstKeys = append(blockFirstKeys, k)
		}
		builder.Add(k, vs)
	}

	builder.FinishBlock()
	id, offset, err := builder.FinishAll(100, 200, 100)
	table, err := OpenTable(stream, id, offset)
	assert.Nil(t, err)
	//fmt.Printf("big %s, small %s\n", table.biggest, table.smallest)
	assert.Equal(t, blockCount, len(table.blockIndex))
	assert.Equal(t, fmt.Sprintf("%016x", 99999), string(table.biggest))
	assert.Equal(t, fmt.Sprintf("%016x", 0), string(table.smallest))
	for i := range blockFirstKeys {
		assert.Equal(t, table.blockIndex[i].Key, blockFirstKeys[i])
	}
}
