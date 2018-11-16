Benefits
* The lease mechanism allows for a single-point TTL logic for several related keys. Not only can the "lease creator"
  set the lease on a key, but any put operation can include the numerical lease ID
* Ability to watch a range of keys
* Range operations in general are helpful for iterative processing needed for Envoy assignments
* Robust and horizontally scalable -- used by kubernetes as datastore

Drawbacks
* Forming structured keys is tedious
* jetcd watch support forces a single key range per watch and requires a blocking `listen` call. Hopefully they'll
  enhance this later to better expose the gRPC etcd3 api for watches
* JSON encoding the key values is awkward
* A watch event is unable to indicate a lease expiration vs a specific deletion. Have to create separate keys to track
  the difference
* jetcd doesn't let you specify the lease ID, which would have been handy to using the envoy instance ID as the lease
  ID
* Lease IDs are numerical (long) so the original plan of using the envoy instance ID for the lease ID didn't work out.
  Could derive a 64-bit hash of the envoy instance ID, but that seems awkward and defeats the 128-bit size of the
  UUID used for envoy instance IDs.