package vsphere_dim

// 接口返回的封装
type CollectionResults struct {
	VMs          map[string]*VM
	Datacenters  map[string]*Datacenter
	Clusters     map[string]*Cluster
	Hosts        map[string]*Host
	Datastores   map[string]*Datastore
	VirtualDisks []*VirtualDisk
}
