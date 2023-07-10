# Library of Flow Graph Elements

The `flow_graph` breaks down into stages, each defined by an overarching
concept. These top-level concepts are complex internally but with simple final
result. Each of the stages is fully separated by concepts; there are no class
references across stage boundaries. As a result, each of the top-level concepts
has multiple possible instantiations that can be selected independently. None
of the classes that satisfy these concepts are necessary; any can be replaced.
Most of the code in the flow graph system, therefore, is library code.

The library is arranged in sections according to top-level concept. The
directory names are abbreviations for the concepts
1. concept `graph_static_specification`, directory `static`
2. concept `execution_platform`, directory `platform`
3. concept `graph_dynamic_specification`, directory `dynamic`
4. concept `execution_graph`, directory `graph`
5. concept `graph_scheduler`, director `scheduler`

More specifically, each subdirectory contains what is produced for that stage,
regardless of its dependency. For example, `ToDynamicReference` produces dynamic
specifications, so it's in that directory.

There is no single unit test for the library as a whole, and only sometimes for
the stages. The `execution_platform` stage, in particular, is (that is, _will
be_) quite extensive and not suitable for a single unit test.