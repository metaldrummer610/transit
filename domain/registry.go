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

import "github.com/spf13/viper"

type configService func(viper *viper.Viper) Service
type configRoute func(viper *viper.Viper) Route

var serviceRegistry map[string]configService = make(map[string]configService)
var routeRegistry map[string]configRoute = make(map[string]configRoute)

// GetServiceMapping finds the configuration function defined for a specific protocol
// along with the configuration values, and creates a new Service
func GetServiceMapping(protocol string, viper *viper.Viper) Service {
	return serviceRegistry[protocol](viper)
}

// GetRouteMapping finds the configuration function defined for a specific protocol
// along with the configuration values, and creates a new Route
func GetRouteMapping(protocol string, viper *viper.Viper) Route {
	return routeRegistry[protocol](viper)
}
