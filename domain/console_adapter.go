// Copyright © 2017 Robbie Diaz <metaldrummer610@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package domain

import (
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// ConsoleAdapter is an Output Adapter that will take any messages and write them to stdout
type ConsoleAdapter struct {
}

func init() {
	routeRegistry["console"] = func(viper *viper.Viper) Route {
		return &ConsoleAdapter{}
	}
}

func (c *ConsoleAdapter) Write(message Message) error {
	Logger().Debug("Console Adapter", zap.String("message", message.String()))
	return nil
}

func (c *ConsoleAdapter) Close() {}
