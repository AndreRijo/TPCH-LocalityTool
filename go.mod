module tpch_locality_tool

go 1.16

require tpch_data_processor v0.0.0

replace potionDB/crdt => ../potionDB/crdt

replace potionDB/shared => ../potionDB/shared

replace tpch_data_processor => ../tpch_data_processor
