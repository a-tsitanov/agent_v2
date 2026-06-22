# Evaluation scripts

## Community backend parity (gate for flipping TEMPORAL_COMMUNITY_BACKEND -> leidenalg)

Run bench_community_backends.py against a representative Neo4j. Flip the
default to leidenalg only when, vs GDS: modularity is within ~5% on the
coarsest level, community count/size distribution is not pathologically
different, AND Neo4j peak heap during detection drops materially.
Until then community_backend stays "gds".
