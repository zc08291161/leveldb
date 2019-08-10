#ifndef ART_DEFINE_H_
#define ART_DEFINE_H_
#ifndef WIN32_DEBUG
#include "pid_index.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif

#ifndef IN
#define IN
#endif

#ifndef OUT
#define OUT
#endif

#ifndef IO
#define IO
#endif

#ifndef likely
#define likely
#endif

#ifndef unlikely
#define unlikely
#endif

#define N_THREADS                       (100)
#define MAX_PREFIX_LEN                  (8)
#define SNAPID_LEN                      (4)


#define END_POSITION_INDEX              (256)
#define SIZE_OF_UNIT64                  (64)
#define SHIFT_NUM_OF_UNIT64             (6)
#define SHIFT_NUM_OF_UNIT8              (3)

#define NODE_4_MAX_CHILDREN_NUM         (4)
#define NODE_16_MAX_CHILDREN_NUM        (16)
#define NODE_48_MAX_CHILDREN_NUM        (48)
#define NODE_256_MAX_CHILDREN_NUM       (256)
#define MAX_CHILDREN_NUM                (256)
#define NODE_48_KEYS_LEN                (256)
#define WRITE_BUFF_SIZE                 (2 << 20) //2MB

// TODO:目前因为外部写接口尚未适配重复切plog，写plog失败重试阈值为1
#define FLUSH_TRY_MAX_FREQUENCY         (1)

#define Second_To_Us                    (1000000)
#define YIELD_OVER_TIME                 (100000) //(100ms)

#define MIN_TABLE_NUM                   (1)
#define MAX_TABLE_NUM                   (4) // max shard migrate to new tree num
#define MIN_SHARD_NUM                   (1) // min shard num
#ifndef WIN32_DEBUG
#define MAX_SHARD_NUM                   (1) // max shard num
#else
#define MAX_SHARD_NUM                   (1) // max shard num //memory is not enough in windows
#endif
#define MAX_BITMAP_IDX                  (4)
#define U64_PER_U8                      (8)

#define SERIALIZE_FLUSH_BUFF_MAX_NUM    (256)
#define SERIALIZE_FLUSH_BUFF_MAX_LEN    (32 << 10) //32KB

/*支持读写并发*/
#define SUPPORT_RW_CONCURRENCY          (1)

#define ART_ROOT_CRC                    (0x1234567887654321) //10 hexadecimal:1311768467139281697
#define ART_MAGIC_FOR_PAGE              (0x9e370001)         //for page verify

#define READ_STATUS                     (0xAB)  //10 hexadecimal:171
#define RELEASE_STATUS                  (0x34)  //10 hexadecimal:52

#define SERIALIZE_MAX_CHIID_NUM         (48) //for calculate serialize reverse memory 
#define SERIALIZE_AVG_CHIID_NUM         (16)
#define MERGE_MAX_CHILD_NUM             (256)
#define MERGE_AVG_CHILD_NUM             (32)
#define ART_MEM_MAX_CHIID_NUM           (48)

#define MAX_VALUE_LEN_IN_LUNMAP         (64)   //lunp should support variable value_len

#define MAX_MERGED_TREE_NUM             (1024)       //(64)

#define MERGE_BUFF_SIZE                 (4 << 20) //4MB,可以考虑是否缩小到2M
#define MERGE_DUS_BUFF_SIZE             (1 << 20) //1MB
#define FLUSH_BUFFER_THRESHOLD          (32 << 10) //32KB
#define FLUSH_BUFFER_THRESHOLD_LV0      (24 << 10) // 0.75*FLUSH_BUFFER_THRESHOLD

#define MAX_FLUSH_BUFFER_NUM            (512) //max batch num, should >= (FLUSH_BUFFER_NUM_THRESHOLD - 1 + MAX_CHILDREN_NUM)
#define FLUSH_BUFFER_NUM_THRESHOLD      (256) //batch num flush threshold, should be adjusted according to plog performace
#define FLUSH_BUFFER_NUM_THRESHOLD_LV0  (192) // pre flush threshold, to make sure there is enough space for snap tree merge. 0.75*FLUSH_BUFFER_NUM_THRESHOLD

#define MAX_SNAP_NODE_NUM               ((SNAPID_LEN + 1) * ART_MEM_MAX_CHIID_NUM)

#define ART_INVALID_CHILD_POS           (0xFFFFFFFF)

#define SIZE_OF_CHILD_OFFSET            (3) // child offset size in fusion node

#define MERGE_BITMAP_SIZE               ((MAX_MERGED_TREE_NUM + SIZE_OF_UNIT64 - 1) / SIZE_OF_UNIT64)
#define MERGE_BITMAP_SIZE_UNIT8         (MAX_MERGED_TREE_NUM >> SHIFT_NUM_OF_UNIT8)

/*define for art memory*/
#define ART_MAX_LEVEL                   (1)
#define L0_MAX_NUM                      (64)
#define L1_MAX_NUM                      (64)
#define L2_MAX_NUM                      (4 + 1) // one big + four small
#define MAX_CONCURRENCY_PER_LEVEL       (5)
#define MAX_MEMTABLE_ART_NUM            (4)
#define MAX_ART_SERIALZIE               (1)
#define MAX_ART_WRITE_BUFF_NUM          (16)
#define MAX_LUN_MAP_KEY_LEN             (64)
#define MAX_NON_LUM_MAP_KEY_LEN         (64)
#define MAX_ART_VALUE_LEN               (64)
#define MAX_ART_DB_NUM                  (4)
#define SERIALIZE_BUFF_SIZE             (4 << 20)
#define SERIALIZE_PATH_NODE_NUM_MIN(ken_len)  (((ken_len) + SNAPID_LEN - 1) * SERIALIZE_AVG_CHIID_NUM + SERIALIZE_MAX_CHIID_NUM)
#define SERIALIZE_PATH_NODE_NUM_MAX(ken_len)  (((ken_len) + SNAPID_LEN) * SERIALIZE_MAX_CHIID_NUM)
#define MAX_SNAP_TREE_NUM               (512)
#define MAX_ART_BATCH_GET               (1)

#define SERIAL_REPORT_THRESHOLD         (10000)
#define COMPACT_REPORT_THRESHOLD        (100000)

//promote return a special token, this is the max len of the token 
#define PROMOTE_MAX_TOKEN_LEN           (10)

/*define for artree write async start*/
#define ARTREE_PERSISTENCE_ALIGN_SIZE   (8 * 1024)
#define ARTREE_TRIGGER_FLUSH_THRESHOLD  (32 * 1024)
#define ARTREE_FLUSH_BUFF_NUM           (ARTREE_TRIGGER_FLUSH_THRESHOLD / ARTREE_PERSISTENCE_ALIGN_SIZE)
#define ARTREE_FLUSH_RESERVER_NUM       (16) // 2
/*define for artree write async end*/

/*define for artree repair start*/
#define ARTREE_FUISON_LEAF_VERIFY_LEN   (1)
#define ARTREE_SEPARTOR_KEY_LEN         (1)
/*define for artree repair end*/

/*define iterator opt start*/
#define ARTREE_ITER_READ_BUFF_SIZE      (16 * 1024)
#define ARTREE_ITER_BUFF_RESERVED_LEN   (1)
/*define iterator opt end*/

#ifndef MY_PID
#define MY_PID PID_INDEX_ART
#endif

#ifdef __cplusplus
}
#endif
#endif

