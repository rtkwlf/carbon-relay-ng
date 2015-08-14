package main

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type RouteConfig interface {
	Dests() []*Destination
	AddDestination(d *Destination)
	DelDestination(index int)
	Update(opts map[string]string) error
}

type BaseRouteConfig struct {
	dests []*Destination
}

func (rc *BaseRouteConfig) Dests() []*Destination {
	return rc.dests
}

func (rc *BaseRouteConfig) AddDestination(d *Destination) {
	rc.dests = append(rc.dests, d)
}

func (rc *BaseRouteConfig) DelDestination(index int) {
	rc.dests = append(rc.dests[:index], rc.dests[index+1:]...)
}

type RuleRouteConfig struct {
	BaseRouteConfig
	Matcher Matcher
}

func (rc *RuleRouteConfig) Update(opts map[string]string) error {
	prefix := rc.Matcher.Prefix
	sub := rc.Matcher.Sub
	regex := rc.Matcher.Regex
	updateMatcher := false

	for name, val := range opts {
		switch name {
		case "prefix":
			prefix = val
			updateMatcher = true
		case "sub":
			sub = val
			updateMatcher = true
		case "regex":
			regex = val
			updateMatcher = true
		default:
			return fmt.Errorf("no such option '%s'", name)
		}
	}
	if updateMatcher {
		matcher, err := NewMatcher(prefix, sub, regex)
		if err != nil {
			return err
		}
		rc.Matcher = *matcher
	}
	return nil
}

type ConsistentHashingRouteConfig struct {
	BaseRouteConfig
}

func (rc *ConsistentHashingRouteConfig) Update(opts map[string]string) error {
	// No configuration to actually update
	return nil
}

type Route interface {
	Dispatch(buf []byte)
	Match(s []byte) bool
	Snapshot() RouteSnapshot
	Key() string
	Flush() error
	Shutdown() error
	DelDestination(index int) error
	UpdateDestination(index int, opts map[string]string) error
	Update(opts map[string]string) error
}

type RouteSnapshot struct {
	Matcher Matcher        `json:"matcher"`
	Dests   []*Destination `json:"destination"`
	Type    string         `json:"type"`
	Key     string         `json:"key"`
}

type baseRoute struct {
	sync.Mutex              // only needed for the multiple writers
	config     atomic.Value // for reading and writing

	key string
}

type RouteSendAllMatch struct {
	baseRoute
}

type RouteSendFirstMatch struct {
	baseRoute
}

type RouteConsistentHashing struct {
	baseRoute
	Hasher ConsistentHasher
}

// NewRouteSendAllMatch creates a sendAllMatch route.
// We will automatically run the route and the given destinations
func NewRouteSendAllMatch(key, prefix, sub, regex string, destinations []*Destination) (Route, error) {
	m, err := NewMatcher(prefix, sub, regex)
	if err != nil {
		return nil, err
	}
	r := &RouteSendAllMatch{baseRoute{sync.Mutex{}, atomic.Value{}, key}}
	r.config.Store(&RuleRouteConfig{BaseRouteConfig{destinations}, *m})
	r.run()
	return r, nil
}

// NewRouteSendFirstMatch creates a sendFirstMatch route.
// We will automatically run the route and the given destinations
func NewRouteSendFirstMatch(key, prefix, sub, regex string, destinations []*Destination) (Route, error) {
	m, err := NewMatcher(prefix, sub, regex)
	if err != nil {
		return nil, err
	}
	r := &RouteSendFirstMatch{baseRoute{sync.Mutex{}, atomic.Value{}, key}}
	r.config.Store(&RuleRouteConfig{BaseRouteConfig{destinations}, *m})
	r.run()
	return r, nil
}

func NewRouteConsistentHashing(key string, destinations []*Destination) (Route, error) {
	r := &RouteConsistentHashing{baseRoute{sync.Mutex{}, atomic.Value{}, key},
		NewConsistentHasher(destinations)}
	r.config.Store(&ConsistentHashingRouteConfig{BaseRouteConfig{destinations}})
	r.run()
	return r, nil
}

func (route *baseRoute) run() {
	conf := route.config.Load().(RouteConfig)
	for _, dest := range conf.Dests() {
		dest.Run()
	}
}

func (route *RouteSendAllMatch) Dispatch(buf []byte) {
	conf := route.config.Load().(*RuleRouteConfig)

	for _, dest := range conf.Dests() {
		if dest.Match(buf) {
			// dest should handle this as quickly as it can
			log.Info("route %s sending to dest %s: %s", route.key, dest.Addr, buf)
			dest.in <- buf
		}
	}
}

func (route *RouteSendFirstMatch) Dispatch(buf []byte) {
	conf := route.config.Load().(*RuleRouteConfig)

	for _, dest := range conf.Dests() {
		if dest.Match(buf) {
			// dest should handle this as quickly as it can
			log.Info("route %s sending to dest %s: %s", route.key, dest.Addr, buf)
			dest.in <- buf
			break
		}
	}
}

func (route *RouteConsistentHashing) Dispatch(buf []byte) {
	conf := route.config.Load().(RouteConfig)
	dest := conf.Dests()[route.Hasher.GetDestinationIndex(buf)]
	log.Info("route %s sending to dest %s: %s", route.key, dest.Addr, buf)
	dest.in <- buf
}

func (route *baseRoute) Key() string {
	return route.key
}

func (route *RouteSendAllMatch) Match(s []byte) bool {
	conf := route.config.Load().(*RuleRouteConfig)
	return conf.Matcher.Match(s)
}

func (route *RouteSendFirstMatch) Match(s []byte) bool {
	conf := route.config.Load().(*RuleRouteConfig)
	return conf.Matcher.Match(s)
}

func (route *RouteConsistentHashing) Match(s []byte) bool {
	return true
}

func (route *baseRoute) Flush() error {
	conf := route.config.Load().(RouteConfig)

	for _, d := range conf.Dests() {
		err := d.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

func (route *baseRoute) Shutdown() error {
	conf := route.config.Load().(RouteConfig)

	destErrs := make([]error, 0)

	for _, d := range conf.Dests() {
		err := d.Shutdown()
		if err != nil {
			destErrs = append(destErrs, err)
		}
	}

	if len(destErrs) == 0 {
		return nil
	}
	errStr := ""
	for _, e := range destErrs {
		errStr += "   " + e.Error()
	}
	return fmt.Errorf("one or more destinations failed to shutdown:" + errStr)
}

// to view the state of the table/route at any point in time
func (route *RouteSendAllMatch) Snapshot() RouteSnapshot {
	conf := route.config.Load().(*RuleRouteConfig)
	dests := make([]*Destination, len(conf.Dests()))
	for i, d := range conf.Dests() {
		dests[i] = d.Snapshot()
	}
	return RouteSnapshot{conf.Matcher, dests, "sendAllMatch", route.key}
}

func (route *RouteSendFirstMatch) Snapshot() RouteSnapshot {
	conf := route.config.Load().(*RuleRouteConfig)
	dests := make([]*Destination, len(conf.Dests()))
	for i, d := range conf.Dests() {
		dests[i] = d.Snapshot()
	}
	return RouteSnapshot{conf.Matcher, dests, "sendFirstMatch", route.key}
}

func (route *RouteConsistentHashing) Snapshot() RouteSnapshot {
	conf := route.config.Load().(RouteConfig)
	dests := make([]*Destination, len(conf.Dests()))
	for i, d := range conf.Dests() {
		dests[i] = d.Snapshot()
	}
	// FIXME: Support hasher
	matcher, _ := NewMatcher("", "", "")
	return RouteSnapshot{*matcher, dests, "consistentHashing", route.key}
}

// Add adds a new Destination to the Route and automatically runs it for you.
// The destination must not be running already!
func (route *baseRoute) Add(dest *Destination) {
	route.Lock()
	defer route.Unlock()
	conf := route.config.Load().(RouteConfig)
	dest.Run()
	conf.AddDestination(dest)
	route.config.Store(conf)
}

func (route *baseRoute) DelDestination(index int) error {
	route.Lock()
	defer route.Unlock()
	conf := route.config.Load().(RouteConfig)
	if index >= len(conf.Dests()) {
		return fmt.Errorf("Invalid index %d", index)
	}
	conf.Dests()[index].Shutdown()
	conf.DelDestination(index)
	route.config.Store(conf)
	return nil
}

func (route *baseRoute) Update(opts map[string]string) error {
	route.Lock()
	defer route.Unlock()
	conf := route.config.Load().(RouteConfig)
	err := conf.Update(opts)
	if err != nil {
		return err
	}
	route.config.Store(conf)
	return nil
}
func (route *baseRoute) UpdateDestination(index int, opts map[string]string) error {
	conf := route.config.Load().(RouteConfig)
	if index >= len(conf.Dests()) {
		return fmt.Errorf("Invalid index %d", index)
	}
	return conf.Dests()[index].Update(opts)
}
