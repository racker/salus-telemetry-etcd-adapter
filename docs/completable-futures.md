The etcd code uses the [jetcd](https://github.com/etcd-io/jetcd), which is the official Java client for etcd v3.
It in turn makes extensive use of Java's [CompletableFuture](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html),
which was introduced in Java 8. In addition to the material below, [this guide](https://www.baeldung.com/java-completablefuture)
provides a good overview of CompletableFuture.

In summary, completable futures provide a reactive, asynchronous programming style which makes sense for database
operations like those of etcd. In other frameworks, these would be called promises which boid down to a 
"do this, then do that" construct.

In addition to jetcd, Spring MVC supports CompletableFuture's where it allows them as a the return type of REST
operations. The presence of that return type triggers Servlet 3's support for asynchronous handling of requests.
As such, all of the REST controller methods that require etcd interaction (currently, all of them) propagate the
CompletableFuture, such as

```java
    @GetMapping("/byType/{agentType}")
    public CompletableFuture<List<AgentInfo>> getAgentsByType(@PathVariable AgentType agentType) {
        return agentsCatalogService.getAgentsByType(agentType);
    }
```

As a side note, the code that interacts with jetcd and CompletableFuture also makes extensive use of Java lambda
expressions and Java 8 streams. The later provides a chaining, reactive-like programming style that blends well
with the reactive style of competable futures.

There's quite a few methods in CompletableFuture, but the main ones used in this code are the following

## thenApply

Performs a mapping operation on the resolved input value. Your code only needs to return a regular value and
CompletableFuture will take of wrapping it another instance that is already resolved.

## thenCompose

Performs a chaining operation by taking a resolved input value and letting you return a new CompletableFuture.