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
	"context"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type mockRetryService struct {
	MaxRetries, Retries int
}

type mockRetryRoute struct {
	Retries int
}

var retryMessages chan Message = make(chan Message, 10)

var MockRetryService = &mockRetryService{}
var MockRetryRoute = &mockRetryRoute{}

func init() {
	serviceRegistry["mockRetry"] = func(v *viper.Viper) Service {
		MockRetryService.MaxRetries = v.GetInt("jitter.attempts")
		return MockRetryService
	}

	routeRegistry["mockRetry"] = func(v *viper.Viper) Route {
		return MockRetryRoute
	}
}

func (m*mockRetryService) Run(ctx context.Context, messages chan Message) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case retry := <-retryMessages:
			Logger().Info("Mock retry read message", zap.Stringer("message", &retry))
			if m.MaxRetries > m.Retries {
				m.Retries++
				messages <- retry
			}
		}
	}

	return nil
}

func (m*mockRetryRoute) Write(message Message) error {
	Logger().Info("Mock retry write message", zap.Stringer("message", &message))
	m.Retries++
	retryMessages <- message
	return nil
}

func (m*mockRetryRoute) Close() {
	MockRetryService.Retries = 0
	MockRetryService.MaxRetries = 0
	MockRetryRoute.Retries = 0

	close(retryMessages)
	retryMessages = make(chan Message, 10)
}
