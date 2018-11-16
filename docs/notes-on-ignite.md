Advantages
* The applications become the cluster members (unless stand-alone nodes are created), so the Ignite cluster can
  scale as the application scales
* It is natively implemented in Java, so fits well with Spring Boot microservices

Drawbacks
* TTL per cache is not ideal since an envoy instance will have keys in several caches that all need to be TTL'ed
  when the envoy drops off
* Continuous queries sounded attractive, but in actuality they are not that useful since they are glorified event
  watchers. You still have to implement an operator to filter the key/value changes. They also don't easily allow for
  for cross-cache queries which is what I was hoping to use for label matching of envoys to configs
* It was easy to configure the boolean for native persistence, but it complained about the cluster needing to be 
  marked up first and it was not intuitive what that meant in my application's case
* Having the cache be part of the application seemed cool at first since it would scale at the app footprint scaled,
  but in reality it's very awkward to update the app code and lose a portion of the cluster in the process. It's just
  not a proper 12-factor, separation of concerns arrangement.