// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"bytes"
	"context"
	"strings"

	"github.com/BurntSushi/toml"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/configpb"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/util/testkit"
)

type testClusterConfig struct {
	*baseTestSuite
}

type mockConfigClient struct {
	conf map[string]map[string]map[string]interface{} // type -> instance -> config
}

func (c *mockConfigClient) GetClusterID(ctx context.Context) uint64 { return 0 }

func (c *mockConfigClient) Create(ctx context.Context, v *configpb.Version, component, componentID, config string) (*configpb.Status, *configpb.Version, string, error) {
	return nil, nil, "", nil
}

func (c *mockConfigClient) GetAll(ctx context.Context) (*configpb.Status, []*configpb.LocalConfig, error) {
	confs := make([]*configpb.LocalConfig, 0, 8)
	for typ, m := range c.conf {
		for instance, conf := range m {
			confData, err := c.encode(conf)
			if err != nil {
				return nil, nil, err
			}
			confs = append(confs, &configpb.LocalConfig{
				Version:     nil,
				Component:   typ,
				ComponentId: instance,
				Config:      confData,
			})
		}
	}

	return &configpb.Status{Code: configpb.StatusCode_OK}, confs, nil
}

func (c *mockConfigClient) Get(ctx context.Context, v *configpb.Version, component, componentID string) (*configpb.Status, *configpb.Version, string, error) {
	confData, err := c.encode(c.conf[component][componentID])
	return &configpb.Status{Code: configpb.StatusCode_OK}, nil, confData, err
}

func (c *mockConfigClient) Update(ctx context.Context, v *configpb.Version, kind *configpb.ConfigKind, entries []*configpb.ConfigEntry) (*configpb.Status, *configpb.Version, error) {
	instance := kind.GetLocal().GetComponentId()
	name, value := entries[0].GetName(), entries[0].GetValue() // suppose that there is only one entry
	for _, m := range c.conf {
		for id, v := range m {
			if id == instance {
				fields := strings.Split(name, ".")
				for i, f := range fields {
					if i < len(fields)-1 {
						v = v[f].(map[string]interface{})
					} else {
						v[f] = value
					}
				}
			}
		}
	}
	return &configpb.Status{Code: configpb.StatusCode_OK}, nil, nil
}

func (s *mockConfigClient) encode(conf map[string]interface{}) (string, error) {
	confBuf := bytes.NewBuffer(nil)
	te := toml.NewEncoder(confBuf)
	if err := te.Encode(conf); err != nil {
		return "", err
	}
	return confBuf.String(), nil
}

func (s *mockConfigClient) decode(conf string) (map[string]interface{}, error) {
	m := make(map[string]interface{})
	_, err := toml.Decode(conf, &m)
	return m, err
}

func (s *mockConfigClient) set(component, componentID, conf string) error {
	if s.conf == nil {
		s.conf = make(map[string]map[string]map[string]interface{})
	}
	if s.conf[component] == nil {
		s.conf[component] = make(map[string]map[string]interface{})
	}
	c, err := s.decode(conf)
	s.conf[component][componentID] = c
	return err
}

func (c *mockConfigClient) Delete(ctx context.Context, v *configpb.Version, kind *configpb.ConfigKind) (*configpb.Status, error) {
	return nil, nil
}

func (c *mockConfigClient) Close() {}

func (s *testClusterConfig) TestClusterConfig(c *C) {
	confClient := &mockConfigClient{}
	executor.GetPDConfigClientFunc = func() (pd.ConfigClient, error) {
		return confClient, nil
	}
	defer func() {
		executor.GetPDConfigClientFunc = config.GetPDConfigClient
	}()

	conf := `[log]
level= "info"
format = "text"`
	c.Assert(confClient.set("tidb", "127.0.0.1:3307", conf), IsNil)
	c.Assert(confClient.set("tidb", "127.0.0.1:3306", conf), IsNil)
	c.Assert(confClient.set("pd", "127.0.0.1:2333", conf), IsNil)

	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery("show config").Sort().Check(
		testkit.Rows("pd 127.0.0.1:2333 log.format text", "pd 127.0.0.1:2333 log.level info",
			"tidb 127.0.0.1:3306 log.format text", "tidb 127.0.0.1:3306 log.level info",
			"tidb 127.0.0.1:3307 log.format text", "tidb 127.0.0.1:3307 log.level info"))
	tk.MustQuery("show config where type='pd'").Sort().Check(
		testkit.Rows("pd 127.0.0.1:2333 log.format text", "pd 127.0.0.1:2333 log.level info"))
	tk.MustQuery("show config where instance='127.0.0.1:3307'").Sort().Check(
		testkit.Rows("tidb 127.0.0.1:3307 log.format text", "tidb 127.0.0.1:3307 log.level info"))

	tk.MustExec("set config '127.0.0.1:2333' log.format='binary'")
	tk.MustQuery("show config").Sort().Check(
		testkit.Rows("pd 127.0.0.1:2333 log.format binary", "pd 127.0.0.1:2333 log.level info",
			"tidb 127.0.0.1:3306 log.format text", "tidb 127.0.0.1:3306 log.level info",
			"tidb 127.0.0.1:3307 log.format text", "tidb 127.0.0.1:3307 log.level info"))

	tk.MustExec("set config '127.0.0.1:3306' log.level='debug'")
	tk.MustQuery("show config").Sort().Check(
		testkit.Rows("pd 127.0.0.1:2333 log.format binary", "pd 127.0.0.1:2333 log.level info",
			"tidb 127.0.0.1:3306 log.format text", "tidb 127.0.0.1:3306 log.level debug",
			"tidb 127.0.0.1:3307 log.format text", "tidb 127.0.0.1:3307 log.level info"))
}
