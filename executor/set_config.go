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

package executor

import (
	"context"
	"github.com/pingcap/tidb/expression"
	"net"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/configpb"
	"github.com/pingcap/parser/mysql"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/util/chunk"
)

var (
	// hack this function for test
	GetPDConfigClientFunc = config.GetPDConfigClient
)

type SetConfigExec struct {
	baseExecutor

	Type     string
	Instance string
	Name     string
	Value    expression.Expression
	pdCli    pd.ConfigClient
}

func (s *SetConfigExec) Open(ctx context.Context) error {
	checker := privilege.GetPrivilegeManager(s.ctx)
	if checker != nil && !checker.RequestVerification(s.ctx.GetSessionVars().ActiveRoles, "", "", "", mysql.SuperPriv) {
		return core.ErrSpecificAccessDenied.GenWithStackByArgs("SET CONFIG")
	}

	if s.Type != "" {
		s.Type = strings.ToLower(s.Type)
		if s.Type != "tikv" && s.Type != "tidb" && s.Type != "pd" {
			return errors.Errorf("invalid config type %v, which should be tikv, tidb or pd", s.Type)
		}
	}
	if s.Instance != "" {
		s.Instance = strings.ToLower(s.Instance)
		if !isValidInstance(s.Instance) {
			return errors.Errorf("invalid instance %v", s.Instance)
		}
	}
	// TODO: check if the config item is valid
	s.Name = strings.ToLower(s.Name)

	pdCli, err := GetPDConfigClientFunc()
	s.pdCli = pdCli
	return err
}

func (s *SetConfigExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	kind := new(configpb.ConfigKind)
	if s.Type != "" {
		kind.Kind = &configpb.ConfigKind_Global{Global: &configpb.Global{Component: s.Type}}
	} else {
		kind.Kind = &configpb.ConfigKind_Local{Local: &configpb.Local{ComponentId: s.Instance}}
	}

	// get the latest version
	stat, ver, _, err := s.pdCli.Get(ctx, config.UnspecifiedVersion, s.Type, s.Instance)
	if err != nil {
		return err
	}
	if stat.GetCode() != configpb.StatusCode_OK {
		return errors.Errorf("pd error, errcode=%v, error=%v", stat.GetCode(), stat.GetMessage())
	}

	val, isNull, err := s.Value.EvalString(s.ctx, chunk.Row{})
	if err != nil {
		return err
	}
	if isNull {
		return errors.Errorf("can't set config to null")
	}
	stat, _, err = s.pdCli.Update(ctx, ver, kind, []*configpb.ConfigEntry{{Name: s.Name, Value: val}})
	if err != nil {
		return err
	}
	if stat.GetCode() != configpb.StatusCode_OK {
		return errors.Errorf("pd error, errcode=%v, error=%v", stat.GetCode(), stat.GetMessage())
	}
	return nil
}

func isValidInstance(instance string) bool {
	var ip, port string
	for i := len(instance) - 1; i >= 0; i-- {
		if instance[i] == ':' {
			ip = instance[:i]
			port = instance[i+1:]
		}
	}
	if port == "" {
		return false
	}
	v := net.ParseIP(ip)
	return v != nil
}
