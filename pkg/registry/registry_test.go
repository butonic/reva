package registry

import (
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
	"gotest.tools/assert"
)

var (
	reg   = New()
	node1 = Node{
		Id:      uuid.New().String(),
		Address: "0.0.0.0:42069",
		Metadata: map[string]string{
			"type": "auth-bearer",
		},
	}

	node2 = Node{
		Id:      uuid.New().String(),
		Address: "0.0.0.0:7777",
		Metadata: map[string]string{
			"type": "auth-basic",
		},
	}

	node3 = Node{Id: uuid.NewString(), Address: "0.0.0.0:8888"}
	node4 = Node{Id: uuid.NewString(), Address: "0.0.0.0:9999"}
)

var services = []Service{
	{
		Name: "auth-provider",
		Nodes: []Node{
			node1,
			node2,
		},
	},
}

var scenarios = []struct {
	name          string // scenario name
	in            string // used to query the registry by service name
	services      []Service
	expectedNodes []Node // expected set of nodes
}{
	{
		name: "single service with 2 nodes",
		in:   "auth-provider",
		services: []Service{
			{Name: "auth-provider", Nodes: []Node{node1, node2}},
		},
		expectedNodes: []Node{node1, node2},
	},
	{
		name: "single service with 2 nodes scaled x2",
		in:   "auth-provider",
		services: []Service{
			{Name: "auth-provider", Nodes: []Node{node1, node2}},
			{Name: "auth-provider", Nodes: []Node{node3, node4}},
		},
		expectedNodes: []Node{node1, node2, node3, node4},
	},
}

func TestGetService(t *testing.T) { // populate the global registry before each test run.
	for _, scenario := range scenarios {
		reg = New()
		for _, service := range scenario.services {
			if err := reg.Add(service); err != nil {
				os.Exit(1)
			}
		}
		t.Run(scenario.name, func(t *testing.T) {
			svc, err := reg.GetService(scenario.in)
			if err != nil {
				t.Error(err)
			}

			totalNodes := 0
			for i := range svc {
				totalNodes += len(svc[i].Nodes)
				for _, node := range svc[i].Nodes {
					if !contains(svc[i].Nodes, node) {
						t.Errorf("unexpected return value: registry does not contain node %s", node)
					}
				}
			}
			assert.Equal(t, len(scenario.expectedNodes), totalNodes)
		})
	}
}

func TestGetServiceInDepth(t *testing.T) {
	for _, scenario := range scenarios {
		// restart registry
		reg = New()
		// register all services
		for i := range scenario.services {
			if err := reg.Add(scenario.services[i]); err != nil {
				os.Exit(1)
			}
		}
		t.Run(scenario.name, func(t *testing.T) {
			services, err := reg.GetService(scenario.in)
			if err != nil {
				t.Error(err)
			}

			for _, service := range services {
				for _, nodes := range service.Nodes {
					fmt.Println(nodes.Address)
				}
			}
		})
	}
}

func contains(a []Node, b Node) bool {
	for i := range a {
		if a[i].Id == b.Id {
			return true
		}
	}
	return false
}
