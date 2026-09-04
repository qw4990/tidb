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
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	tidbserver "github.com/pingcap/tidb/pkg/server"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit/testenv"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
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

	server, cfg := startTiDBServer(t)
	require.True(t, cfg.Status.ReportStatus)
	statusOn, statusAddr := server.GetStatusServerAddr()
	require.False(t, statusOn)
	require.Empty(t, statusAddr)

	var buf bytes.Buffer
	goroutineProfile := pprof.Lookup("goroutine")
	require.NotNil(t, goroutineProfile)
	require.NoError(t, goroutineProfile.WriteTo(&buf, 2))

	dump := buf.String()
	require.Contains(t, dump, "goroutine ")
	require.Contains(t, dump, "github.com/pingcap/tidb/pkg/server.(*Server).startNetworkListener")
	backgroundGoroutines := []struct {
		taskName   string
		goroutines []string
	}{
		{
			taskName: "HTTPServer",
			goroutines: []string{
				"github.com/pingcap/tidb/pkg/server.(*Server).startHTTPServer",
				"github.com/pingcap/tidb/pkg/server.(*Server).startStatusServerAndRPCServer",
			},
		},
	}
	for _, backgroundGoroutine := range backgroundGoroutines {
		for _, goroutine := range backgroundGoroutine.goroutines {
			require.NotContains(t, dump, goroutine, "%s background goroutine should not be started", backgroundGoroutine.taskName)
		}
	}
	t.Logf("TiDB goroutine dump in diagnostic mode:\n%s", dump)
}

func startTiDBServer(t *testing.T) (*tidbserver.Server, *config.Config) {
	t.Helper()
	if kerneltype.IsNextGen() {
		testenv.UpdateConfigForNextgen(t)
	}

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	t.Cleanup(view.Stop)

	session.DisableStats4Test()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	t.Cleanup(dom.Close)

	cfg := config.NewConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = 0
	cfg.Socket = ""
	cfg.Status.ReportStatus = true
	cfg.Status.StatusPort = 0

	server, err := tidbserver.NewServer(cfg, tidbserver.NewTiDBDriver(store))
	require.NoError(t, err)
	server.SetDomain(dom)
	dom.InfoSyncer().SetSessionManager(server)

	runErr := make(chan error, 1)
	go func() {
		runErr <- server.Run(nil)
	}()
	select {
	case <-tidbserver.RunInGoTestChan:
	case err := <-runErr:
		server.Close()
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		server.Close()
		require.FailNow(t, "timed out waiting for TiDB server to start")
	}

	t.Cleanup(func() {
		server.Close()
		select {
		case err := <-runErr:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			require.Fail(t, "timed out waiting for TiDB server to stop")
		}
	})
	return server, cfg
}

func enableServerRunInGoTest(t *testing.T) {
	t.Helper()
	originalRunInGoTest := tidbserver.RunInGoTest
	originalRunInGoTestChan := tidbserver.RunInGoTestChan
	tidbserver.RunInGoTest = true
	tidbserver.RunInGoTestChan = make(chan struct{})
	t.Cleanup(func() {
		tidbserver.RunInGoTest = originalRunInGoTest
		tidbserver.RunInGoTestChan = originalRunInGoTestChan
	})
}
