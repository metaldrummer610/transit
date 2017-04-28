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
	"bytes"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"net/http"
)

type httpRoute struct {
	endpoint    string
	contentType string
}

func init() {
	routeRegistry["http"] = func(viper *viper.Viper) Route {
		return &httpRoute{
			endpoint:    viper.GetString("endpoint"),
			contentType: viper.GetString("contentType"),
		}
	}
}

func (h*httpRoute) Write(message Message) error {
	body, err := proto.Marshal(&message)

	if err != nil {
		Logger().Error("Failed to write message to NSQ topic!",
			zap.Error(err),
		)
		return err
	}

	Logger().Debug("Writing an http message",
		zap.String("endpoint", h.endpoint),
	)
	_, err = http.Post(h.endpoint, h.contentType, bytes.NewBuffer(body))

	if err != nil {
		Logger().Error("Failed to POST message to endpoint!",
			zap.String("endpoint", h.endpoint),
			zap.Error(err),
		)
	}
	return err
}

func (h*httpRoute) Close() {}
