// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"fmt"
	"html/template"
	"io"
	"io/ioutil"
	"regexp"
	"strings"
)

type TypeTmplInfo struct {
	TypeName   string
	GoTypeName string
}

func (t TypeTmplInfo) MulFn(l, r string) string {
	return l + "." + t.GoTypeName + "*=" + r
}

func genRowBasedTyped(w io.Writer) error {
	t, err := ioutil.ReadFile("row_based_typed_tmpl.go")
	if err != nil {
		return err
	}
	s := string(t)

	// Replace all simple tokens.
	s = strings.Replace(s, "_GOTYPE", "{{ .GoTypeName }}", -1)
	s = strings.Replace(s, "_TYPE", "{{ .TypeName }}", -1)

	// Replace all functions.
	mulFnRe := regexp.MustCompile(`_MULFN\((.*),(.*)\)`)
	s = mulFnRe.ReplaceAllString(s, `{{ .MulFn "$1" "$2" }}`)

	fmt.Println("Preprocessed template:\n", s)

	tmpl, err := template.New("row_based_typed").Parse(s)
	if err != nil {
		return err
	}

	typesToGenerate := []TypeTmplInfo{
		{
			TypeName:   "Int64",
			GoTypeName: "int64",
		},
		{
			TypeName:   "Float64",
			GoTypeName: "float64",
		},
	}
	return tmpl.Execute(w, typesToGenerate)
}

func init() {
	registerGenerator("row_based_typed.gen.go", genRowBasedTyped)
}
