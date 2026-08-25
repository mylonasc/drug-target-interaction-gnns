## TarKG dataset

The initial version of this code was implemented with drugbank in mind. 

A newer and larger biomedical KG is TarKG. 

Resources:

|type|link|
|----|----|
|paper| https://academic.oup.com/bioinformatics/article/40/10/btae598/7818343?login=false |
|dataset| https://tarkg.ddtmlab.org/download| 

### Core KG files

`TarKG_nodes.csv` and `TarKG_edges.csv` contain the unified TarKG node and edge
tables. Their mapping companions retain provenance back to the source knowledge
graphs:

|file|contents|
|----|--------|
|`TarKG_nodes_mapping.csv`|Maps each unified TarKG node to source identifiers. Includes the TarKG `index`, `unify_id`, entity `kind`, source IDs (`kgid`, `dbid`), source labels (`db_source`, `source`), entity `name`, and source-KG provenance (`kg_index`, `kg`).|
|`TarKG_edges_mapping.csv`|Maps each unified TarKG edge to source graph records. Includes the TarKG edge `index`, original head/tail IDs and types (`node1`, `node1_type`, `node2`, `node2_type`), original `relation`, source `db_source`, source-KG provenance (`kg_index`, `kg`), and `change`, where `1` means the original triple head/tail order was swapped and `0` means it was unchanged.|

The loader caches files under `~/.datasets/tarkg_data` by default and disables
TLS verification by default because the TarKG host currently serves an expired
certificate.
