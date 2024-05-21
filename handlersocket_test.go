/*
Copyright 2011 Brian Ketelsen

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

/*
CREATE  TABLE `hstest`.`kvs` (
  `id` VARCHAR(255) NOT NULL ,
  `content` VARCHAR(255) NULL ,
  PRIMARY KEY (`id`) )
ENGINE = InnoDB DEFAULT CHARSET=utf8;
*/

package handlersocket

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

var addr = "udb1.d3"

func TestOpenIndex(t *testing.T) {
	// Create new instance
	hs := New(context.Background())

	// Connect to database
	err := hs.Connect(addr, 9998, 9999)
	defer func() { _ = hs.Close }()

	require.NoError(t, err)

	err = hs.OpenIndex(1, "hstest", "kvs", "PRIMARY", "id", "content")
	require.NoError(t, err)
}

func TestDelete(t *testing.T) {
	hs := New(context.Background())

	// Connect to database
	err := hs.Connect(addr, 9998, 9999)
	defer func() { _ = hs.Close }()

	require.NoError(t, err)

	// id is varchar(255), content is text
	err = hs.OpenIndex(3, "hstest", "kvs", "PRIMARY", "id", "content")
	require.NoError(t, err)

	var keys, newvals []string

	keys = make([]string, 1)
	newvals = make([]string, 0)

	for n := 1; n < 10; n++ {
		keys[0] = "blue" + strconv.Itoa(n)
		_, err := hs.Modify(3, "=", 10, 0, "D", keys, newvals)
		require.NoError(t, err)
	}
}

func TestWrite(t *testing.T) {
	hs := New(context.Background())

	// Connect to database
	err := hs.Connect(addr, 9998, 9999)
	defer func() { _ = hs.Close }()
	require.NoError(t, err)

	// id is varchar(255), content is text
	err = hs.OpenIndex(3, "hstest", "kvs", "PRIMARY", "id", "content")
	require.NoError(t, err)

	err = hs.Insert(3, "blue1", "a quick brown fox jumped over a lazy dog")
	require.NoError(t, err)
	err = hs.Insert(3, "blue2", "a quick brown fox jumped over a lazy dog")
	require.NoError(t, err)

	// Chinese Data
	err = hs.Insert(3, "chinese", "中文測試資料")
	require.NoError(t, err)

}

func TestModify(t *testing.T) {
	hs := New(context.Background())

	// Connect to database
	err := hs.Connect(addr, 9998, 9999)
	defer func() { _ = hs.Close }()

	require.NoError(t, err)

	// id is varchar(255), content is text
	err = hs.OpenIndex(3, "hstest", "kvs", "PRIMARY", "id", "content")
	require.NoError(t, err)

	err = hs.Insert(3, "blue3", "a quick brown fox jumped over a lazy dog")
	require.NoError(t, err)
	err = hs.Insert(3, "blue4", "a quick brown fox jumped over a lazy dog")
	require.NoError(t, err)

	var keys, newvals []string

	keys = make([]string, 1)
	newvals = make([]string, 2)

	keys[0] = "blue3"
	newvals[0] = "blue7"
	newvals[1] = "some new thing"

	_, err = hs.Modify(3, "=", 1, 0, "U", keys, newvals)
	require.NoError(t, err)

	keys[0] = "blue4"
	newvals[0] = "blue5"
	newvals[1] = "My new value!"
	_, err = hs.Modify(3, "=", 1, 0, "U", keys, newvals)
	require.NoError(t, err)

}

func TestRead(t *testing.T) {
	hs := New(context.Background())

	// Connect to database
	err := hs.Connect(addr, 9998, 9999)
	defer func() { _ = hs.Close }()

	require.NoError(t, err)

	err = hs.OpenIndex(1, "hstest", "kvs", "PRIMARY", "id", "content")
	require.NoError(t, err)

	found, err := hs.Find(1, "=", 1, 0, "blue7")
	require.NoError(t, err)
	require.Len(t, found, 1, "Expected one record for blue7")

	found, err = hs.Find(1, "=", 1, 0, "chinese")
	require.NoError(t, err)
	require.Len(t, found, 1, "Expected one record for chinese")
	require.Equal(t, "中文測試資料", found[0].Data["content"])
}

func BenchmarkOpenIndex(b *testing.B) {
	b.StopTimer()
	hs := New(context.Background())
	defer func() { _ = hs.Close }()

	_ = hs.Connect(addr, 9998, 9999)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_ = hs.OpenIndex(1, "hstest", "kvs", "PRIMARY", "id", "content")
	}
}
func BenchmarkFind(b *testing.B) {
	b.StopTimer()
	hs := New(context.Background())
	defer func() { _ = hs.Close }()

	_ = hs.Connect(addr, 9998, 9999)
	_ = hs.OpenIndex(1, "hstest", "kvs", "PRIMARY", "id", "content")

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _ = hs.Find(1, "=", 1, 0, "brian")
	}
}
