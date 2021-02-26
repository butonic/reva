package registry

import (
	"fmt"
	"sync"

	"github.com/mitchellh/mapstructure"
)

/*
	Some conceptual notes:
	- Should a registry run as a reva service? If so, we would need to add a protobuf interface
		in order to send messages to the service.
	- The go-micro style of using a registry does not require a server. But what if we want to use an in-memory one?
		should we have a global (god forbid) with the current registry state?.
	- The drawbacks of using an in-memory registry is that its contents would not leave the process boundary. Hence the
		addition of a protobuf api, so we can bypass the process memory address. But again, this requires to have the
		service running as its own process. Consider adding a registry as part of the reva processes.
	- In order to have a transition from working exclusively with hardcoded addresses and a dynamic registry we need to
		 add a feature flag to use either/or strategies.
*/

// Registry provides with means for dynamically registering services.
type Registry interface {
	// Add registers a service on the registry. Repeated names is allowed, services are distinguished by their metadata.
	Add(Service) error

	// GetService retrieves a service and all of its nodes by service name. It returns []*Service because we can have
	// multiple versions of the same service running alongside each others.
	GetService(string) ([]*Service, error)
}

var (
	// Registry is a work in progress in-memory global registry.
	GlobalRegistry Registry = New()
)

type config struct {
	Services map[string][]*Service{}, `mapstructure:"services"`
}

func (c *config) init() {
	if len(c.Services) == 0 {
		c.Services = map[string][]*Service{}
	}
}

// registry implements the Registry interface.
type registry struct {
	// m protects async access to the services map.
	sync.Mutex
	// services map a service name with a set of nodes.
	services map[string][]*Service
}

// Add implements the Registry interface.
func (r *registry) Add(service Service) error {
	r.Lock()
	defer r.Unlock()

	r.services[service.Name] = append(r.services[service.Name], &service)
	return nil
}

// GetService implements the Registry interface. There is currently no load balance being done, but it should not be
// hard to add.
func (r *registry) GetService(name string) ([]*Service, error) {
	r.Lock()
	defer r.Unlock()

	services := make([]*Service, 0)
	if serv, ok := r.services[name]; ok {
		for i := range serv {
			services = append(services, serv[i])
		}
	}
	return services, nil
}

// Service represents a running service with multiple nodes.
type Service struct {
	Name  string
	Nodes []Node
}

type Node struct {
	// Id uniquely identifies the node.
	Id string

	// Address where the given node is running.
	Address string

	// metadata is used in order to differentiate services implementations. For instance an AuthProvider service could
	// have multiple implementations, basic, bearer ..., metadata would be used to select the service type depending on
	// its implementation.
	Metadata map[string]string
}

func parseConfig(m map[string]interface{}) (*config, error) {
	c := &config{}
	if err := mapstructure.Decode(m, c); err != nil {
		return nil, err
	}
	return c, nil
}

// New returns an implementation of the Registry interface.
func New(m map[string]interface{}) (Registry, error) {
	c, err := parseConfig(m)
	if err != nil {
		return nil, err
	}
	c.init()
	return &registry{
		services: map[string][]*Service{},
	}, nil
}

func (n Node) String() string {
	return fmt.Sprintf("%v-%v", n.Id, n.Address)
}
