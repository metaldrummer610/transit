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

package pkg

import (
	"context"
	"fmt"
	"github.com/metaldrummer610/transit/domain"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"strings"
)

const RETRY_NAME = "_retry"

type router struct {
	ctx      context.Context
	messages chan domain.Message
	routes   map[string]domain.Route
	services map[string]domain.Service
}

func NewRouter(ctx context.Context) *router {
	r := &router{
		ctx:      ctx,
		messages: make(chan domain.Message),
		routes:   make(map[string]domain.Route),
		services: make(map[string]domain.Service),
	}

	r.configureRoutes(viper.Sub("routes"))
	r.configureServices(viper.Sub("services"))

	// Retry route & service
	retryConfig := viper.Sub("retry")
	protocol := retryConfig.GetString("protocol")

	domain.Logger().Debug("Configuring retry with:", zap.Any("config", retryConfig.AllSettings()))
	r.configureRoute(RETRY_NAME, protocol, retryConfig)
	r.configureService(RETRY_NAME, protocol, retryConfig)

	return r
}

func (r *router) AllRoutes() []string {
	keys := make([]string, 0, len(r.routes))
	for k := range r.routes {
		keys = append(keys, k)
	}

	return keys
}

func (r *router) AllServices() []string {
	keys := make([]string, 0, len(r.services))
	for k := range r.services {
		keys = append(keys, k)
	}

	return keys
}

func (r *router) Run() {
	for i := 0; i < 100000; i++ {
		go r.run()
	}
}

func (r *router) run() {
	domain.Logger().Info("Router started!")

	for {
		domain.Logger().Debug("Inside for loop")
		select {
		case <-r.ctx.Done():
			domain.Logger().Info("Context done! Leaving router main", zap.Error(r.ctx.Err()))
			goto done
		case msg := <-r.messages:
			domain.Logger().Debug("Reading message destined for", zap.String("destination", msg.Destination))
			dest := r.routes[strings.ToLower(msg.Destination)]

			if dest == nil {
				domain.Logger().Error("Unable to find destination!",
					zap.String("destination", msg.Destination))
				break
			}

			if err := dest.Write(msg); err != nil {
				domain.Logger().Warn("Message failed to write. Retrying...",
					zap.String("destination", msg.Destination),
					zap.Error(err),
				)

				if err := r.routes[RETRY_NAME].Write(msg); err != nil {
					domain.Logger().Error("Message failed to retry.",
						zap.String("destination", msg.Destination),
						zap.Error(err),
					)
				}
			}
		}
	}

done:
	domain.Logger().Warn("Got to the end of the Run function")
	r.Close()
}

func (r*router) Close() {
	for _, route := range r.routes {
		route.Close()
	}
}

func (r *router) configureRoutes(v *viper.Viper) {
	var mappings map[string]interface{}
	if err := v.Unmarshal(&mappings); err != nil {
		domain.Logger().Error("Unable to unmarshal route configuration!", zap.Error(err))
	}

	domain.Logger().Debug("Configuring Routes", zap.Any("keys", mappings))

	for routeAlias := range mappings {
		domain.Logger().Debug("Configuring ", zap.String("alias", routeAlias))

		protocol := v.GetString(fmt.Sprintf("%s.protocol", routeAlias))
		r.configureRoute(routeAlias, protocol, v.Sub(routeAlias))
	}
}

func (r *router) configureServices(v *viper.Viper) {
	var mappings map[string]interface{}
	if err := v.Unmarshal(&mappings); err != nil {
		domain.Logger().Error("Unable to unmarshal service configuration!", zap.Error(err))
	}

	domain.Logger().Debug("Configuring Services", zap.Any("keys", mappings))

	for serviceAlias := range mappings {
		domain.Logger().Debug("Configuring ", zap.String("alias", serviceAlias))

		protocol := v.GetString(fmt.Sprintf("%s.protocol", serviceAlias))
		r.configureService(serviceAlias, protocol, v.Sub(serviceAlias))
	}
}
func (r *router) configureService(alias string, protocol string, v *viper.Viper) {
	service := domain.GetServiceMapping(protocol, v)
	r.services[alias] = service

	go service.Run(r.ctx, r.messages)
}

func (r *router) configureRoute(alias string, protocol string, v *viper.Viper) {
	route := domain.GetRouteMapping(protocol, v)
	r.routes[alias] = route
}
