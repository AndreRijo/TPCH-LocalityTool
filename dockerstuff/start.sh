#!/bin/bash
echo $SERVERS;

umask 000;
tpch_data_processor/main/main --data_loc=$DATA_LOC --sf=$SF --order_rate=$ORDER_LOCALITY --item_rate=$ITEM_LOCALITY --one_rem_rate=$ONE_REM_RATE --two_rem_rate=$TWO_REM_RATE --two_diff_rem_rate=$TWO_DIFF_REG_REM_RATE --n_upd_files=$N_UPD_FILES