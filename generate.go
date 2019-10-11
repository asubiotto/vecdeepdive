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
	"io"
	"os"
)

func main() {
	if err := generate(); err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
}

func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

var generators = make(map[string]func(io.Writer) error)

func registerGenerator(fileName string, generateFn func(io.Writer) error) {
	if _, ok := generators[fileName]; ok {
		panic(fmt.Sprintf("already registered generator for %s", fileName))
	}
	generators[fileName] = generateFn
}

func generate() error {
	for fileName, generateFn := range generators {
		if err := removeIfExists(fileName); err != nil {
			return err
		}
		f, err := os.Create(fileName)
		if err != nil {
			return err
		}
		defer f.Close()

		if _, err := f.WriteString("// This is generated code. DO NOT EDIT.\n"); err != nil {
			return err
		}

		if err := generateFn(f); err != nil {
			return err
		}
	}
	return nil
}
