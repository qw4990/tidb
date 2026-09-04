// Copyright 2026 PingCAP, Inc.
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

package diagnosticmode_test

import (
	"bytes"
	"runtime/pprof"
	"testing"

	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	tidbserver "github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/server/tests/servertestkit"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
)

func TestDumpTiDBServerGoroutinesInDiagnosticMode(t *testing.T) {
	if !intest.InTest {
		t.Skip("diagnosticmode.SetForTest requires the intest build tag")
	}
	testsetup.SetupForCommonTest()
	restoreMode := diagnosticmode.SetForTest(true)
	t.Cleanup(restoreMode)
	enableServerRunInGoTest(t)

	require.True(t, diagnosticmode.Enabled())

	servertestkit.CreateTidbTestSuite(t)

	var buf bytes.Buffer
	goroutineProfile := pprof.Lookup("goroutine")
	require.NotNil(t, goroutineProfile)
	require.NoError(t, goroutineProfile.WriteTo(&buf, 2))

	dump := buf.String()
	require.Contains(t, dump, "goroutine ")
	require.Contains(t, dump, "github.com/pingcap/tidb/pkg/server.(*Server).startNetworkListener")
	t.Logf("TiDB goroutine dump in diagnostic mode:\n%s", dump)
}

func enableServerRunInGoTest(t *testing.T) {
	t.Helper()
	originalRunInGoTest := tidbserver.RunInGoTest
	originalRunInGoTestChan := tidbserver.RunInGoTestChan
	tidbserver.RunInGoTest = true
	t.Cleanup(func() {
		tidbserver.RunInGoTest = originalRunInGoTest
		tidbserver.RunInGoTestChan = originalRunInGoTestChan
	})
}
