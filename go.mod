module tpch_data_processor

go 1.16

replace potionDB => ../potionDB

replace tpch_client => ../tpch_client

require (
	potionDB v0.0.0-00010101000000-000000000000
	tpch_client v0.0.0-00010101000000-000000000000
)
