// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunkenc

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

type triple struct {
	st, t int64
	v     float64
}

func TestChunk(t *testing.T) {
	for enc, nc := range map[Encoding]func() Chunk{
		EncXOR:   func() Chunk { return NewXORChunk() },
		EncXORv2: func() Chunk { return NewXORv2Chunk() },
	} {
		t.Run(fmt.Sprintf("%v", enc), func(t *testing.T) {
			c := nc()
			testChunk(t, c)
		})
	}
}

func compatNewAppenderV2(c Chunk) (_ func() (AppenderV2, error), stSupported bool) {
	ca, stSupported := c.(supportsAppenderV2)
	if stSupported {
		return ca.AppenderV2, true
	}
	// To simplify tests, v1 appender simulate v2 by ignoring ST data.
	return func() (AppenderV2, error) {
		a, err := c.Appender()
		return &ignoreSTAppenderV2{Appender: a}, err
	}, false
}

func testChunk(t *testing.T, c Chunk) {
	newAppenderFn, stSupported := compatNewAppenderV2(c)

	app, err := newAppenderFn()
	require.NoError(t, err)

	var exp []triple
	var (
		ts  = int64(1234123324)
		sts = ts - 15000
		v   = 1243535.123
	)
	for i := range 3 { // 300 {
		sts += int64(rand.Intn(10000) + 1)
		ts += int64(rand.Intn(10000) + 1)
		if i%2 == 0 {
			v += float64(rand.Intn(1000000))
		} else {
			v -= float64(rand.Intn(1000000))
		}

		// Start with a new appender every 10th sample. This emulates starting
		// appending to a partially filled chunk.
		if i%10 == 0 {
			app, err = newAppenderFn()
			require.NoError(t, err)
		}

		app.Append(sts, ts, v)
		exp = append(exp, triple{st: sts, t: ts, v: v})
	}

	// 1. Expand iterator in simple case.
	it1 := c.Iterator(nil)
	var res1 []triple
	for i := 0; it1.Next() == ValFloat; i++ {
		ts, v := it1.At()
		st := it1.AtST()
		if !stSupported {
			// Supplement ST from expectations for formats that does not support ST.
			st = exp[i].st
		}
		res1 = append(res1, triple{st: st, t: ts, v: v})
	}
	require.NoError(t, it1.Err())
	require.Equal(t, exp, res1)

	// 2. Expand second iterator while reusing first one.
	it2 := c.Iterator(it1)
	var res2 []triple
	for i := 0; it2.Next() == ValFloat; i++ {
		ts, v := it2.At()
		st := it2.AtST()
		if !stSupported {
			// Supplement ST from expectations for formats that does not support ST.
			st = exp[i].st
		}
		res2 = append(res2, triple{st: st, t: ts, v: v})
	}
	require.NoError(t, it2.Err())
	require.Equal(t, exp, res2)

	// 3. Test iterator Seek.
	mid := len(exp) / 2

	it3 := c.Iterator(nil)
	var res3 []triple
	require.Equal(t, ValFloat, it3.Seek(exp[mid].t))
	// Below ones should not matter.
	require.Equal(t, ValFloat, it3.Seek(exp[mid].t))
	require.Equal(t, ValFloat, it3.Seek(exp[mid].t))
	ts, v = it3.At()
	st := it3.AtST()
	if !stSupported {
		// Supplement ST from expectations for formats that does not support ST.
		st = exp[mid].st
	}
	res3 = append(res3, triple{st: st, t: ts, v: v})

	for i := mid + 1; it3.Next() == ValFloat; i++ {
		ts, v := it3.At()
		st := it3.AtST()
		if !stSupported {
			// Supplement ST from expectations for formats that does not support ST.
			st = exp[i].st
		}
		res3 = append(res3, triple{st: st, t: ts, v: v})
	}
	require.NoError(t, it3.Err())
	require.Equal(t, exp[mid:], res3)
	require.Equal(t, ValNone, it3.Seek(exp[len(exp)-1].t+1))
}

func TestPool(t *testing.T) {
	p := NewPool()
	for _, tc := range []struct {
		name     string
		encoding Encoding
		expErr   error
	}{
		{
			name:     "xor",
			encoding: EncXOR,
		},
		{
			name:     "histogram",
			encoding: EncHistogram,
		},
		{
			name:     "float histogram",
			encoding: EncFloatHistogram,
		},
		{
			name:     "invalid encoding",
			encoding: EncNone,
			expErr:   errors.New(`invalid chunk encoding "none"`),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := p.Get(tc.encoding, []byte("test"))
			if tc.expErr != nil {
				require.EqualError(t, err, tc.expErr.Error())
				return
			}

			require.NoError(t, err)

			var b *bstream
			switch tc.encoding {
			case EncHistogram:
				b = &c.(*HistogramChunk).b
			case EncFloatHistogram:
				b = &c.(*FloatHistogramChunk).b
			default:
				b = &c.(*XORChunk).b
			}

			require.Equal(t, &bstream{
				stream: []byte("test"),
				count:  0,
			}, b)

			b.count = 1
			require.NoError(t, p.Put(c))
			require.Equal(t, &bstream{
				stream: nil,
				count:  0,
			}, b)
		})
	}

	t.Run("put bad chunk wrapper", func(t *testing.T) {
		// When a wrapping chunk poses as an encoding it can't be converted to, Put should skip it.
		c := fakeChunk{
			encoding: EncXOR,
			t:        t,
		}
		require.NoError(t, p.Put(c))
	})
	t.Run("put invalid encoding", func(t *testing.T) {
		c := fakeChunk{
			encoding: EncNone,
			t:        t,
		}
		require.EqualError(t, p.Put(c), `invalid chunk encoding "none"`)
	})
}

type fakeChunk struct {
	Chunk

	encoding Encoding
	t        *testing.T
}

func (c fakeChunk) Encoding() Encoding {
	return c.encoding
}

func (c fakeChunk) Reset([]byte) {
	c.t.Fatal("Reset should not be called")
}
