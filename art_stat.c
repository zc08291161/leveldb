#include "art_stat.h"

#ifdef __cplusplus
extern "C" {
#endif

/*init the statistic info or art*/
void init_art_stat_info(int32_t apply_type)
{
    int32_t i = 0;
    int32_t j = 0;
    /*memtable*/
    atomic64_set(&art_mem_stat[apply_type].insert_leaf_num, 0);
    atomic64_set(&art_mem_stat[apply_type].replace_leaf_num, 0);

    /*serialize*/
    atomic64_set(&art_serialize_stat[apply_type].serialize_key_num, 0);
    atomic64_set(&art_serialize_stat[apply_type].serialize_leaf_num, 0);
    atomic64_set(&art_serialize_stat[apply_type].write_plog_size, 0);
    atomic64_set(&art_serialize_stat[apply_type].write_plog_max_size, 0);
    
    atomic64_set(&art_serialize_stat[apply_type].serialize_total_num, 0);
    atomic64_set(&art_serialize_stat[apply_type].serialize_total_time, 0);
    atomic64_set(&art_serialize_stat[apply_type].serialize_read_plog_num, 0);
    atomic64_set(&art_serialize_stat[apply_type].serialize_read_plog_time, 0);
    atomic64_set(&art_serialize_stat[apply_type].serialize_write_plog_num, 0);
    atomic64_set(&art_serialize_stat[apply_type].serialize_write_plog_time, 0);
    atomic64_set(&art_serialize_stat[apply_type].serialize_yeild_num, 0);
    atomic64_set(&art_serialize_stat[apply_type].serialize_yeild_time, 0);
    /*compaction*/
    for (i = 0; i < FlushLevelType_BUTT; ++i)
    {
        atomic64_set(&art_compaction_stat[apply_type][i].compation_total_num, 0);
        atomic64_set(&art_compaction_stat[apply_type][i].compation_total_time, 0);
        atomic64_set(&art_compaction_stat[apply_type][i].compation_key_num, 0);
        atomic64_set(&art_compaction_stat[apply_type][i].compation_over_key_num, 0);
        atomic64_set(&art_compaction_stat[apply_type][i].compation_leaf_num, 0);
        atomic64_set(&art_compaction_stat[apply_type][i].compation_over_leaf_num, 0);
        atomic64_set(&art_compaction_stat[apply_type][i].compation_write_plog_num, 0);
        atomic64_set(&art_compaction_stat[apply_type][i].compation_write_plog_size, 0);
        atomic64_set(&art_compaction_stat[apply_type][i].compation_write_plog_max_size, 0);
    }
    /*op promote*/
    atomic64_set(&art_op_promote_stat.op_promote_leaf_num, 0);
    /*callback*/
    for (i = 0; i < FlushLevelType_BUTT; ++i)
    {
        for(j = 0; j < CALLBACK_TYPE_NUM; ++j)
        {
            atomic64_set(&art_callback_stat[apply_type][i].callback_num[j], 0);
        }
    }

    return;
}

#ifdef __cplusplus
}
#endif


