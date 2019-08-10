#ifndef ART_STAT_H
#define ART_STAT_H

#ifndef ARTREE_DEBUG
#include "lvos.h"
#endif
#include "art_pri.h"
#include "art_pub.h"
#include "art_flush.h"
#ifndef WIN32_DEBUG
#include "dpax_atomic_redef.h"
#endif
#ifdef __cplusplus
extern "C" {
#endif

/*memtable stat*/
typedef struct
{
    atomic64_t insert_leaf_num;  /*insert memtable leaf num*/
    atomic64_t replace_leaf_num; /*replace memtable leaf num*/
}art_mem_stat_t;

art_mem_stat_t art_mem_stat[kPelagoDBBusinessTypeButt];

/*serialize stat*/
typedef struct
{
    atomic64_t serialize_key_num;
    atomic64_t serialize_leaf_num;
    atomic64_t write_plog_size;
    atomic64_t write_plog_max_size;

    atomic64_t serialize_total_num;
    atomic64_t serialize_total_time;
    atomic64_t serialize_read_plog_num;
    atomic64_t serialize_read_plog_time;
    atomic64_t serialize_write_plog_num;
    atomic64_t serialize_write_plog_time;
    atomic64_t serialize_yeild_num;
    atomic64_t serialize_yeild_time;
}art_serialize_stat_t;

art_serialize_stat_t art_serialize_stat[kPelagoDBBusinessTypeButt];

atomic64_t art_tolerate_snap_disorder;

/*callback stat of each level*/
typedef struct
{
    atomic64_t callback_num[CALLBACK_TYPE_NUM]; //count number of callback invocation of each callback interface type
}art_callback_stat_t;

art_callback_stat_t art_callback_stat[kPelagoDBBusinessTypeButt][FlushLevelType_BUTT];

/*compaction stat for per level*/
typedef struct
{
    atomic64_t compation_total_num;		//compaction执行次数
    atomic64_t compation_total_time;	//compaction执行总时间
    atomic64_t compation_key_num;		//参与compaction key总数
    atomic64_t compation_over_key_num;	//compaction生成新树包含key总数
    atomic64_t compation_leaf_num;		//参与compaction snapid总数
    atomic64_t compation_over_leaf_num;	//compaction生成新树包含snapid总数
    atomic64_t compation_write_plog_num;//写plog总次数
    atomic64_t compation_write_plog_size;//写plog总大小
    atomic64_t compation_write_plog_max_size;//暂未使用
    //读写、yield总时间及次数
    atomic64_t compaction_read_plog_num;
    atomic64_t compaction_read_plog_time;
    atomic64_t compaction_write_plog_num;
    atomic64_t compaction_write_plog_time;
    atomic64_t compaction_yeild_num;
    atomic64_t compaction_yeild_time;

    //for callback 总时间及次数
    atomic64_t compaction_arbitrate_num;
    atomic64_t compaction_arbitrate_time;

    atomic64_t compaction_gc_record_num;
    atomic64_t compaction_gc_record_time;
    atomic64_t compaction_promote_num;
    atomic64_t compaction_promote_time;
    atomic64_t compaction_migration_num;
    atomic64_t compaction_migration_time;

    //buff_wait、async_wait总时间及次数 
    atomic64_t buff_full_wait_time;
    atomic64_t buff_full_wait_num;

    atomic64_t async_flush_wait_time;
    atomic64_t async_flush_wait_num;
}art_compaction_stat_t;

art_compaction_stat_t art_compaction_stat[kPelagoDBBusinessTypeButt][FlushLevelType_BUTT];

//for read buff hit ration
typedef struct
{
    atomic64_t  total_read_buff_num;
    atomic64_t  hit_read_buff_num;
}art_read_buff_stat_t;

art_read_buff_stat_t art_read_buff_stat[kPelagoDBBusinessTypeButt];


/*optable */
typedef struct
{
    atomic64_t op_promote_num;
    atomic64_t op_promote_leaf_num;
    atomic64_t op_promote_max_leaf_num;
}art_op_promote_stat_t;

art_op_promote_stat_t art_op_promote_stat;


void init_art_stat_info(int32_t apply_type);

#ifdef __cplusplus
}
#endif

#endif

